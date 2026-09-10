"""SnowLuma 影子适配器 — 以 NapCat 适配器的名义向 SnowLuma 环境透传 action API。

定位（影子适配器）：
    MaiBot 官方 SnowLuma 适配器只注册了固定清单的 ``@API``，没有 NapCat 适配器的
    通用 action 入口 ``adapter.napcat.action.call``，导致面向 NapCat 编写的生态插件
    （如表情回应插件 cateye_set_msg_emoji_like）在 SnowLuma 环境下无法下发
    ``set_msg_emoji_like`` 等动作。而 SnowLuma 本体（OneBot 服务端）实际已实现这些动作。

    本插件自建一条到 SnowLuma 服务端的 WebSocket 连接（SnowLuma 服务端支持多客户端
    并发，见 ``WsServerConnections.connections`` 为 Map，不影响官方适配器的连接），
    以「影子」方式提供 NapCat 适配器的 API 名字，让下游插件零改动运行在 SnowLuma 上：

    1. ``adapter.napcat.action.call``                —— 影子主入口：与 NapCat 适配器
       同名同签名（``action_name`` + ``params``，返回原始响应），下游插件零改动；
    2. ``adapter.napcat.message.set_msg_emoji_like`` —— NapCat 兼容名（可配置关闭）；
    3. ``adapter.snowluma.action.call``              —— SnowLuma 原生命名透传；
    4. ``adapter.snowluma.message.set_msg_emoji_like`` —— SnowLuma 原生命名语义化封装。

自动失效：
    对应官方实装进度：https://github.com/Mai-with-u/MaiBot-SnowLuma-Adapter/issues/12
    （feat: 官方 SnowLuma 适配器增加 ``adapter.napcat.action.call`` 透传）。
    插件加载时（及延后复查、配置更新时）通过 ``ctx.api.list()`` 探测影子目标 API：
    一旦发现被其他插件注册——
    - 官方 SnowLuma 适配器实装透传 feat（issue #12）→ 本插件整体失效；
    - NapCat 适配器在场（同名冲突，短名歧义）→ 本插件整体失效；
    失效后不再维持 WebSocket 连接，所有 API handler 以明确错误拒绝服务，
    提示停用本插件。

实现要点：
    - 连接协议与官方适配器一致：``ws://{server}:{port}?access_token={token}``，
      ``{"action", "params", "echo"}`` 请求帧，按 ``echo`` 回填 Future；
    - 服务端每 30s 心跳 ping，aiohttp 默认自动 pong，无需客户端心跳；
    - 入站非 echo 帧（事件推送）一律忽略：本插件只发 action、只认 echo 响应，
      事件仍由官方适配器单路进入 MaiBot，不会造成重复消息；
    - action 超时按官方适配器同样处理：视为链路异常，断开并触发重连。
"""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlencode
from uuid import uuid4

from aiohttp import ClientSession, ClientTimeout, ClientWebSocketResponse, WSMsgType, WSServerHandshakeError
from maibot_sdk import API, Field, MaiBotPlugin, PluginConfigBase

# ==================== 常量 ====================

# 配置版本：与 _manifest.json 的 version 保持同步。
# MaiBot 1.2.3+ 强制要求插件配置提供 plugin.config_version，缺失会导致插件初始化失败。
SUPPORTED_CONFIG_VERSION = "0.2.1"

TOKEN_ERROR_MESSAGE = "token不正确，请在 snowluma-shadow-adapter 插件配置中设置与 SnowLuma WebUI 一致的访问令牌"

# SnowLuma 本体对 token 错误的 retcode 表现（与官方适配器判定一致）
_TOKEN_ERROR_RETCODES = {1401, 401, 403}

# 本插件身份（与 _manifest.json 的 id 保持一致）
SELF_PLUGIN_ID = "github.cateye.snowluma-shadow-adapter"

# 影子目标：官方 SnowLuma 适配器实装透传 feat 的跟踪 issue（实装后本插件失效）
OFFICIAL_FEAT_ISSUE_URL = "https://github.com/Mai-with-u/MaiBot-SnowLuma-Adapter/issues/12"

# 本插件注册的 NapCat 风格影子 API（用于冲突探测）
_SHADOWED_API_NAMES = (
    "adapter.napcat.action.call",
    "adapter.napcat.message.set_msg_emoji_like",
)

# 其他提供方身份
OFFICIAL_ADAPTER_PLUGIN_ID = "maibot-team.snowluma-adapter"
NAPCAT_ADAPTER_PLUGIN_ID = "maibot-team.napcat-adapter"

# 延迟复查间隔（秒）：规避插件加载顺序导致 on_load 时探测不到其他插件 API
_CONFLICT_RECHECK_DELAY_SEC = 15.0


class SnowLumaTokenError(RuntimeError):
    """SnowLuma token 配置不正确。"""


# ==================== 配置模型 ====================


class PluginSectionConfig(PluginConfigBase):
    """插件自身配置（plugin 配置节）。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(
        default=True,
        description="是否启用插件",
        json_schema_extra={
            "label": "启用插件",
            "hint": "插件总开关",
        },
    )
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="配置版本（与插件版本同步，用于检查配置文件是否需要更新）",
        json_schema_extra={
            "disabled": True,
            "hidden": True,
            "label": "配置版本",
            "hint": "配置版本，勿改",
        },
    )


class SnowLumaClientConfig(PluginConfigBase):
    """SnowLuma 连接配置。"""

    __ui_label__ = "SnowLuma 连接"
    __ui_icon__ = "settings_ethernet"
    __ui_order__ = 1

    server: str = Field(
        default="127.0.0.1",
        description="SnowLuma WebSocket 服务地址（与官方适配器一致）",
        json_schema_extra={
            "label": "服务地址",
            "hint": "SnowLuma 服务地址",
        },
    )
    port: int = Field(
        default=3006,
        description="SnowLuma WebSocket 服务端口（默认 3006，与 SnowLuma WebUI 一致）",
        json_schema_extra={
            "label": "服务端口",
            "hint": "服务端口，默认3006",
        },
    )
    token: str = Field(
        default="",
        description="SnowLuma 访问令牌（若 SnowLuma WebUI 启用了 token 校验则必填，与 WebUI 中一致）",
        json_schema_extra={"input_type": "password", "placeholder": "可留空",
                           "label": "访问令牌",
                           "hint": "访问令牌，校验用",
                           },
    )
    reconnect_delay_sec: float = Field(
        default=5.0,
        description="连接断开后的重连等待秒数",
        json_schema_extra={
            "label": "重连等待（秒）",
            "hint": "断线重连间隔秒",
        },
    )
    action_timeout_sec: float = Field(
        default=10.0,
        description="单个 action 的响应超时秒数",
        json_schema_extra={
            "label": "action 超时（秒）",
            "hint": "动作响应超时秒",
        },
    )


class BridgeSectionConfig(PluginConfigBase):
    """影子行为配置。"""

    __ui_label__ = "影子"
    __ui_icon__ = "theater_comedy"
    __ui_order__ = 2

    register_napcat_generic_api: bool = Field(
        default=True,
        description=(
            "启用影子主入口 adapter.napcat.action.call（NapCat 适配器通用 action 入口的影子，"
            "下游插件零改动）。关闭后该入口调用将报错"
        ),
        json_schema_extra={
            "label": "启用 NapCat 通用 action 影子入口",
            "hint": "启用 NapCat 通用入口",
        },
    )
    register_napcat_compat_api: bool = Field(
        default=True,
        description=(
            "启用 NapCat 兼容名 adapter.napcat.message.set_msg_emoji_like（供表情回应等插件的"
            "回退逻辑按该名字调用）。关闭后该入口调用将报错"
        ),
        json_schema_extra={
            "label": "启用表情回应兼容入口",
            "hint": "启用表情兼容入口",
        },
    )
    action_allowlist: List[str] = Field(
        default_factory=list,
        description="允许透传的 action 白名单（每行一个 action 名），对两个透传入口同时生效。留空表示不限制",
        json_schema_extra={
            "label": "action 白名单",
            "hint": "透传 action 白名单",
        },
    )


class BridgeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    snowluma: SnowLumaClientConfig = Field(default_factory=SnowLumaClientConfig)
    bridge: BridgeSectionConfig = Field(default_factory=BridgeSectionConfig)


# ==================== 插件主体 ====================


class SnowLumaShadowAdapterPlugin(MaiBotPlugin):
    """SnowLuma 影子适配器：自建 WebSocket 连接，以 NapCat 名义透传 OneBot 风格 action。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = BridgeConfig

    def __init__(self) -> None:
        super().__init__()
        self._session: Optional[ClientSession] = None
        self._ws: Optional[ClientWebSocketResponse] = None
        self._connection_task: Optional[asyncio.Task[None]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._response_pool: Dict[str, asyncio.Future[Dict[str, Any]]] = {}
        self._invalidated_reason: str = ""  # 非空 = 影子已失效，所有入口拒绝服务
        self._conflict_recheck_task: Optional[asyncio.Task[None]] = None

    # ==================== 公开 API ====================

    @API("adapter.napcat.action.call", description="调用任意 OneBot 动作（SnowLuma 影子适配器透传）", version="1", public=True)
    async def api_napcat_action_call(self, action_name: str = "", params: Any = None) -> Dict[str, Any]:
        """影子主入口：与 NapCat 适配器 ``adapter.napcat.action.call`` 同名同签名。

        返回 SnowLuma 的原始响应字典（``{"status", "retcode", "data", ...}``），
        调用方按 ``status == 'ok'`` 或 ``retcode == 0`` 判定成败，
        与 NapCat 适配器行为一致，下游插件零改动。
        """
        self._ensure_shadow_active()
        if not bool(self._load_settings().bridge.register_napcat_generic_api):
            raise ValueError("影子入口 adapter.napcat.action.call 已在配置中禁用（bridge.register_napcat_generic_api）")
        return await self._passthrough_action_call(action_name, params)

    @API(
        "adapter.napcat.message.set_msg_emoji_like",
        description="设置消息表情回应（NapCat 兼容名，由 SnowLuma 影子适配器提供）",
        version="1",
        public=True,
    )
    async def api_set_msg_emoji_like_compat(self, **kwargs: Any) -> Dict[str, Any]:
        """NapCat 兼容名入口：让下游插件按 ``adapter.napcat.message.set_msg_emoji_like``
        调用时命中本影子适配器（而非 Host 层解析失败）。
        """
        self._ensure_shadow_active()
        if not bool(self._load_settings().bridge.register_napcat_compat_api):
            raise ValueError("影子入口 adapter.napcat.message.set_msg_emoji_like 已在配置中禁用（bridge.register_napcat_compat_api）")
        return await self.api_set_msg_emoji_like(**kwargs)

    @API("adapter.snowluma.action.call", description="调用任意 SnowLuma OneBot 动作（透传）", version="1", public=True)
    async def api_action_call(self, action_name: str = "", params: Any = None) -> Dict[str, Any]:
        """SnowLuma 原生命名透传入口（不会与其他适配器重名）。"""
        self._ensure_shadow_active()
        return await self._passthrough_action_call(action_name, params)

    @API(
        "adapter.snowluma.message.set_msg_emoji_like",
        description="设置消息表情回应（SnowLuma）",
        version="1",
        public=True,
    )
    async def api_set_msg_emoji_like(self, **kwargs: Any) -> Dict[str, Any]:
        """设置消息表情回应（语义化封装，message_id 允许负数）。"""
        self._ensure_shadow_active()
        params = self._api_params(kwargs)
        return await self._call_action(
            "set_msg_emoji_like",
            {
                "message_id": self._normalize_int(params.get("message_id"), "message_id"),
                "emoji_id": str(params.get("emoji_id") or "").strip(),
                "set": bool(params.get("set", True)),
            },
        )

    async def _passthrough_action_call(self, action_name: str, params: Any) -> Dict[str, Any]:
        """透传公共逻辑：校验 action 名与白名单后下发。"""
        normalized_action = str(action_name or "").strip()
        if not normalized_action:
            raise ValueError("action_name 不能为空")
        self._ensure_action_allowed(normalized_action)
        raw_params = params if isinstance(params, Mapping) else {}
        return await self._call_action(normalized_action, dict(raw_params))

    # ==================== 失效判定 ====================

    def _ensure_shadow_active(self) -> None:
        """影子已失效时拒绝服务（官方实装透传 feat / NapCat 适配器在场）。"""
        if self._invalidated_reason:
            raise RuntimeError(
                f"SnowLuma 影子适配器已失效（{self._invalidated_reason}）。"
                f"请停用本插件，改用官方适配器提供的同名 API（进度见 {OFFICIAL_FEAT_ISSUE_URL}）"
            )

    async def _evaluate_conflicts(self) -> None:
        """探测影子目标 API 是否已被其他插件注册，命中则本插件整体失效。

        - 官方 SnowLuma 适配器实装透传 feat（issue #12）→ 失效（本插件的使命完成）；
        - NapCat 适配器在场 → 同名 API 短名歧义，会破坏调用方 → 失效。
        """
        entries = await self._list_api_entries()
        conflicts: List[Tuple[str, str]] = []
        for entry in entries:
            name = self._entry_field(entry, "name")
            plugin_id = self._entry_field(entry, "plugin_id")
            if not name or name not in _SHADOWED_API_NAMES:
                continue
            if plugin_id and plugin_id != SELF_PLUGIN_ID:
                conflicts.append((name, plugin_id))
        if not conflicts:
            if self._invalidated_reason:
                self.ctx.logger.info("影子冲突已消失，但本插件保持失效状态，建议重启 MaiBot 以恢复影子入口")
            return

        has_official = any(pid == OFFICIAL_ADAPTER_PLUGIN_ID for _, pid in conflicts)
        has_napcat = any(pid == NAPCAT_ADAPTER_PLUGIN_ID for _, pid in conflicts)
        detail = "；".join(f"{name} ← {pid}" for name, pid in conflicts)
        if has_official:
            reason = f"官方 SnowLuma 适配器已实装透传 feat（issue {OFFICIAL_FEAT_ISSUE_URL}）"
        elif has_napcat:
            reason = "检测到 NapCat 适配器在场，同名 API 会造成短名解析歧义"
        else:
            reason = "影子目标 API 已被其他插件注册"
        message = f"{reason}：{detail}"
        if self._invalidated_reason != message:
            self._invalidated_reason = message
            self.ctx.logger.warning("SnowLuma 影子适配器自动失效：%s", message)
            self.ctx.logger.warning("影子适配器已停止服务并断开连接；请在插件管理中停用本插件")
            await self._stop_connection()

    async def _list_api_entries(self) -> List[Any]:
        """读取 Host API 注册表（ctx.api.list()），兼容同步/异步返回。"""
        try:
            listed = self.ctx.api.list()
            if inspect.isawaitable(listed):
                listed = await listed
            return list(listed) if listed else []
        except Exception as exc:
            self.ctx.logger.debug("读取 API 注册表失败（跳过冲突探测）：%s", exc)
            return []

    @staticmethod
    def _entry_field(entry: Any, field: str) -> str:
        """兼容 dict / 对象两种 API 元信息形态。"""
        if isinstance(entry, Mapping):
            return str(entry.get(field) or "").strip()
        return str(getattr(entry, field, "") or "").strip()

    async def _delayed_conflict_recheck(self) -> None:
        """延后复查冲突：规避加载顺序导致 on_load 时其他插件 API 尚未注册。"""
        await asyncio.sleep(_CONFLICT_RECHECK_DELAY_SEC)
        await self._evaluate_conflicts()

    # ==================== 生命周期 ====================

    async def on_load(self) -> None:
        """插件加载：检查配置版本、探测冲突（命中则失效），否则启动连接循环。"""
        self._check_config_version()
        await self._evaluate_conflicts()
        if not self._invalidated_reason:
            await self._restart_connection_if_needed()
            self._conflict_recheck_task = asyncio.create_task(
                self._delayed_conflict_recheck(), name="snowluma-shadow-conflict-recheck"
            )

    async def on_unload(self) -> None:
        """插件卸载：停止连接循环并清理资源。"""
        if self._conflict_recheck_task is not None:
            self._conflict_recheck_task.cancel()
            try:
                await self._conflict_recheck_task
            except asyncio.CancelledError:
                pass
            self._conflict_recheck_task = None
        await self._stop_connection()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        """配置更新：应用新配置、重新探测冲突并按需重启连接。"""
        if scope != "self":
            return
        self.set_plugin_config(config_data)
        await self._evaluate_conflicts()
        if not self._invalidated_reason:
            await self._restart_connection_if_needed()

    # ==================== 连接管理 ====================

    def _load_settings(self) -> BridgeConfig:
        return self.config  # type: ignore[return-value]

    async def _restart_connection_if_needed(self) -> None:
        """按当前配置重启连接循环。"""
        await self._stop_connection()
        if self._invalidated_reason:
            return
        if not bool(self._load_settings().plugin.enabled):
            self.ctx.logger.info("SnowLuma 影子适配器保持空闲状态（插件未启用）")
            return
        self._stop_event = asyncio.Event()
        self._connection_task = asyncio.create_task(
            self._run_connection_loop(), name="snowluma-shadow-adapter-loop"
        )

    async def _stop_connection(self) -> None:
        """停止连接循环并清理资源。"""
        if self._stop_event is not None:
            self._stop_event.set()
        task = self._connection_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._connection_task = None
        await self._disconnect()

    async def _run_connection_loop(self) -> None:
        """维持到 SnowLuma 的 WebSocket 连接（断线自动重连）。"""
        while self._stop_event is not None and not self._stop_event.is_set():
            settings = self._load_settings()
            listen_task: Optional[asyncio.Task[None]] = None
            try:
                await self._connect(settings)
                listen_task = asyncio.create_task(self._listen(), name="snowluma-shadow-adapter-listen")
                await self._verify_connection(listen_task)
                self.ctx.logger.info(
                    "SnowLuma 影子适配器已连接: ws://%s:%s（与官方适配器并存，互不影响）",
                    settings.snowluma.server,
                    settings.snowluma.port,
                )
                await listen_task
            except asyncio.CancelledError:
                raise
            except SnowLumaTokenError:
                self.ctx.logger.warning(TOKEN_ERROR_MESSAGE)
            except WSServerHandshakeError as exc:
                if exc.status in {401, 403}:
                    self.ctx.logger.warning(TOKEN_ERROR_MESSAGE)
                else:
                    self.ctx.logger.warning("SnowLuma 影子适配器连接异常，稍后重试: %s", exc)
            except Exception as exc:
                self.ctx.logger.warning("SnowLuma 影子适配器连接异常，稍后重试: %s", exc)
            finally:
                if listen_task is not None and not listen_task.done():
                    listen_task.cancel()
                    try:
                        await listen_task
                    except asyncio.CancelledError:
                        pass
                await self._disconnect()

            if self._stop_event is None or self._stop_event.is_set():
                break
            await asyncio.sleep(max(1.0, settings.snowluma.reconnect_delay_sec))

    async def _connect(self, settings: BridgeConfig) -> None:
        """建立 WebSocket 连接（token 走 access_token 查询参数，与官方适配器一致）。"""
        timeout = ClientTimeout(total=10)
        self._session = ClientSession(timeout=timeout)
        base_url = f"ws://{settings.snowluma.server}:{settings.snowluma.port}"
        url = f"{base_url}?{urlencode({'access_token': settings.snowluma.token})}" if settings.snowluma.token else base_url
        self._ws = await self._session.ws_connect(url)

    async def _verify_connection(self, listen_task: asyncio.Task[None]) -> None:
        """连接后先用 get_login_info 验证 token，并确保验证期间连接没有提前断开。"""
        verify_task = asyncio.create_task(self._call_action("get_login_info", {}), name="snowluma-shadow-verify")
        done, _ = await asyncio.wait({verify_task, listen_task}, return_when=asyncio.FIRST_COMPLETED)
        if verify_task in done:
            response = verify_task.result()
            if response.get("retcode") in _TOKEN_ERROR_RETCODES:
                raise SnowLumaTokenError(TOKEN_ERROR_MESSAGE)
            status = str(response.get("status") or "").lower()
            if status and status != "ok":
                raise RuntimeError(f"SnowLuma 影子适配器连通性验证失败: {response.get('wording') or response.get('message')}")
            return
        verify_task.cancel()
        try:
            await verify_task
        except asyncio.CancelledError:
            pass
        raise SnowLumaTokenError(TOKEN_ERROR_MESSAGE)

    async def _disconnect(self) -> None:
        """关闭 WebSocket 和未完成的动作 Future。"""
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        for future in self._response_pool.values():
            if not future.done():
                future.cancel()
        self._response_pool.clear()

    async def _listen(self) -> None:
        """监听 SnowLuma 推送：只处理 echo 响应帧，事件推送一律忽略。"""
        if self._ws is None:
            return
        async for ws_message in self._ws:
            if ws_message.type == WSMsgType.TEXT:
                self._handle_text_payload(ws_message.data)
                continue
            if ws_message.type == WSMsgType.BINARY:
                continue
            if ws_message.type in {WSMsgType.CLOSED, WSMsgType.ERROR}:
                break

    def _handle_text_payload(self, raw_payload: str) -> None:
        """处理文本帧：echo 帧回填 Future；其余（事件推送）忽略。"""
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        echo = str(payload.get("echo") or "").strip()
        if not echo:
            return
        future = self._response_pool.pop(echo, None)
        if future is not None and not future.done():
            future.set_result(payload)

    async def _call_action(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """调用 SnowLuma OneBot 风格动作接口（与官方适配器 _call_action 同构）。"""
        if self._ws is None:
            raise RuntimeError("SnowLuma WebSocket 尚未连接（snowluma-shadow-adapter）")
        settings = self._load_settings()
        echo = uuid4().hex
        future: asyncio.Future[Dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._response_pool[echo] = future

        payload = {"action": action, "params": params, "echo": echo}
        await self._ws.send_str(json.dumps(payload, ensure_ascii=False))
        try:
            return await asyncio.wait_for(future, timeout=max(1.0, settings.snowluma.action_timeout_sec))
        except asyncio.TimeoutError as exc:
            timeout_seconds = max(1.0, settings.snowluma.action_timeout_sec)
            # 与官方适配器一致：echo 超时视为链路异常，断开旧连接触发重连
            self.ctx.logger.warning(
                "SnowLuma 影子适配器 action 等待响应超时，准备断开旧连接并触发重连: action=%s timeout=%.1fs",
                action,
                timeout_seconds,
            )
            await self._disconnect()
            raise TimeoutError(f"SnowLuma action {action} 响应超时（{timeout_seconds:.1f}s）") from exc
        finally:
            self._response_pool.pop(echo, None)

    # ==================== 辅助 ====================

    @staticmethod
    def _api_params(kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        """兼容直接传参和 NapCat 风格 ``params`` 包装。"""
        raw_params = kwargs.get("params", kwargs)
        if raw_params is None:
            return {}
        if not isinstance(raw_params, Mapping):
            raise ValueError("params 必须是字典")
        return dict(raw_params)

    def _ensure_action_allowed(self, action_name: str) -> None:
        """按白名单配置校验 action（留空 = 不限制）。"""
        allowlist = [str(x).strip() for x in (self._load_settings().bridge.action_allowlist or []) if str(x).strip()]
        if allowlist and action_name not in allowlist:
            raise ValueError(f"action {action_name!r} 不在影子适配器白名单内（bridge.action_allowlist）")

    @staticmethod
    def _normalize_int(value: Any, field_name: str) -> int:
        """规范化任意整数（允许负数）。message_id 是 32 位有符号回绕值。"""
        try:
            return int(str(value).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} 必须是整数") from exc

    def _check_config_version(self) -> None:
        """配置版本提示（Runner 已按默认值自动补齐缺失字段，这里仅记录）。"""
        try:
            raw = self.get_plugin_config_data()
            current = str((raw.get("plugin") or {}).get("config_version") or "").strip()
        except Exception:
            return
        if current and current != SUPPORTED_CONFIG_VERSION:
            self.ctx.logger.info(
                "检测到旧版配置（config_version=%s，当前支持 %s），缺失字段已按默认值自动补齐",
                current,
                SUPPORTED_CONFIG_VERSION,
            )


def create_plugin() -> SnowLumaShadowAdapterPlugin:
    """创建 SnowLuma 影子适配器插件实例。"""
    return SnowLumaShadowAdapterPlugin()
