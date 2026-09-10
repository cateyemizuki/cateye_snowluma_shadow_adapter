# SnowLuma 影子适配器（SnowLuma Shadow Adapter）

作者：cateye。以 **NapCat 适配器的名义**向 SnowLuma 环境透传 action API 的独立 MaiBot 插件。

> **失效声明**：本插件是对 [MaiBot-SnowLuma-Adapter issue #12](https://github.com/Mai-with-u/MaiBot-SnowLuma-Adapter/issues/12)
> （feat: 官方适配器增加 `adapter.napcat.action.call` 透传）的**临时补位**。
> 官方适配器实装该 feat 后，本插件会**自动检测并失效停用**（见下文「自动失效机制」）。

## 为什么需要它

MaiBot 官方 SnowLuma 适配器只注册了固定清单的 `@API`，没有通用 action 入口，导致面向 NapCat 适配器编写的生态插件（如表情回应插件 `cateye_set_msg_emoji_like`）在 SnowLuma 环境下无法下发 `set_msg_emoji_like`（贴表情）等动作——调用在 Host 层就因「API 未注册」失败，根本到不了 SnowLuma。而 SnowLuma 本体（OneBot 服务端）**已经实现**这些动作。

本插件自建一条到 SnowLuma 服务端的 WebSocket 连接，以「影子」方式提供 NapCat 适配器的 API 名字，让下游插件**零改动**运行在 SnowLuma 上。

**为什么不直接调官方适配器？** 适配器内部的 `_call_action()` 是其他插件实例的私有方法，插件之间无法调用；MaiBot 的 `ctx.api.call` 按名称在全局注册表解析，`adapter.napcat.*` 前缀只是命名惯例、不是路由规则。

**会不会影响官方适配器？** 不会。SnowLuma 服务端的 `WsServerConnections` 用 Map 管理多个并发 WebSocket 客户端（每个连接独立鉴权/心跳），本插件与官方适配器并存互不影响。事件仍由官方适配器单路进入 MaiBot——本插件只发 action、只认 echo 响应，忽略一切事件推送，不会造成重复消息。

## 注册的 API

| API 名 | 说明 |
|---|---|
| `adapter.napcat.action.call` | **影子主入口**：NapCat 适配器通用 action 入口的同名同签名影子，下游插件零改动 |
| `adapter.napcat.message.set_msg_emoji_like` | NapCat 兼容名（可配置关闭），供表情回应等插件的回退逻辑调用 |
| `adapter.snowluma.action.call` | SnowLuma 原生命名透传（永不与其他适配器重名） |
| `adapter.snowluma.message.set_msg_emoji_like` | SnowLuma 原生命名语义化封装（`message_id` 允许负数） |

调用示例：

```python
resp = await self.ctx.api.call(
    "adapter.napcat.action.call",
    action_name="set_msg_emoji_like",
    params={"message_id": -5238091734, "emoji_id": "12951", "set": True},
)
# resp == {"status": "ok", "retcode": 0, "data": null, ...}
```

## 自动失效机制

插件在加载时、加载 15 秒后（规避插件加载顺序）、以及配置更新时，通过 `ctx.api.list()` 探测影子目标 API 是否已被其他插件注册。命中任一冲突即**整体失效**：

| 冲突来源 | 含义 | 处理 |
|---|---|---|
| `maibot-team.snowluma-adapter` | 官方 SnowLuma 适配器已实装透传 feat（[issue #12](https://github.com/Mai-with-u/MaiBot-SnowLuma-Adapter/issues/12)），本插件使命完成 | 自动失效 |
| `maibot-team.napcat-adapter` | NapCat 适配器在场，同名 API 短名解析歧义，会破坏调用方 | 自动失效 |

失效后的行为：不再维持 WebSocket 连接，所有 API handler 以明确错误拒绝服务（错误信息含 issue #12 链接），日志提示停用本插件。失效状态不会自动恢复（恢复需重启 MaiBot 或重载插件），避免运行中途 API 入口消失导致调用方状态错乱。

## 配置

```toml
[plugin]
enabled = true
config_version = "0.2.0"

[snowluma]
server = "127.0.0.1"   # 与 SnowLuma 所在主机一致
port = 3006            # 默认 3006，与 SnowLuma WebUI 的 WebSocket 端口一致
token = ""             # SnowLuma WebUI 启用了 token 校验时必填
reconnect_delay_sec = 5.0
action_timeout_sec = 10.0

[bridge]
register_napcat_generic_api = true  # 影子主入口 adapter.napcat.action.call
register_napcat_compat_api = true   # NapCat 兼容名 adapter.napcat.message.set_msg_emoji_like
action_allowlist = []               # 透传白名单，留空不限制；如 ["set_msg_emoji_like", "get_login_info"]
```

连接协议与官方适配器一致：`ws://{server}:{port}?access_token={token}`。连接建立后会先发 `get_login_info` 验证 token，错误时日志提示 `token不正确...`。

## 与表情回应插件（cateye_set_msg_emoji_like）联动

影子主入口与表情回应插件的现有调用路径（`adapter.napcat.action.call`）**完全同名**，所以：

1. **代码零改动**：`_apply_emoji_like` 保持原样即可命中影子入口；
2. **仅需解除硬依赖**：cateye 的 `_manifest.json` 声明了 `maibot-team.napcat-adapter` 插件依赖，缺失时插件不会被加载。SnowLuma-only 环境删除 `dependencies` 中该项（或整个数组置空）。

接收侧（表情回应通知翻译）无需改动：官方 SnowLuma 适配器已把原始 payload 按 NapCat 兼容字段名 `napcat_notice_payload` 透传，cateye 的翻译 hook 开箱即用。

### 可选：更稳健的三入口回退（防御官方未来改签名）

若希望 cateye 同时兼容「官方适配器已实装」「影子适配器」「真 NapCat」三种环境，可把 `_apply_emoji_like` 换为：

```python
    async def _apply_emoji_like(self, stream_id: str, message_id: str, emoji_id: int) -> tuple[bool, str]:
        """贴表情：依次尝试 NapCat 通用入口 → SnowLuma 原生入口 → NapCat 兼容名，返回 (ok, error)。"""
        del stream_id
        params = self._replacer.build_set_emoji_like_params(message_id, emoji_id)
        last_error = ""
        for api_name in (
            "adapter.napcat.action.call",                 # NapCat 适配器 / 影子主入口
            "adapter.snowluma.action.call",               # 影子原生命名入口
            "adapter.napcat.message.set_msg_emoji_like",  # NapCat 专用 API / 影子兼容名
        ):
            response = await self._call_adapter_api(api_name, params)
            if response is None:
                continue  # 该 API 名未注册或 Host 层失败，尝试下一个入口
            ok, error = self._judge_action_response(response)
            if ok:
                return True, ""
            last_error = f"{api_name}: {error}"
        return False, last_error or "无可用适配器入口"

    async def _call_adapter_api(
        self, api_name: str, params: Dict[str, Any]
    ) -> Optional[Mapping[str, Any]]:
        """调用适配器 API；名称未注册（Host 解析失败 / 目标插件异常包装）返回 None 以便回退。"""
        try:
            response = await self.ctx.api.call(api_name, action_name="set_msg_emoji_like", params=params)
        except Exception as exc:
            self.ctx.logger.debug("API %s 调用异常：%s", api_name, exc)
            return None
        if not isinstance(response, Mapping) or response.get("success") is False:
            return None
        return response

    @staticmethod
    def _judge_action_response(response: Mapping[str, Any]) -> tuple[bool, str]:
        """按 OneBot 惯例判定动作响应成败（status=='ok' 或 retcode==0）。"""
        ok = str(response.get("status") or "").lower() == "ok" or response.get("retcode") == 0
        if ok:
            return True, ""
        err = str(response.get("wording") or response.get("message") or response.get("error") or "")
        return False, err or "贴表情失败"
```

## 注意事项

1. **失效优先**：官方适配器实装 [issue #12](https://github.com/Mai-with-u/MaiBot-SnowLuma-Adapter/issues/12) 或 NapCat 适配器在场时，本插件自动失效（见「自动失效机制」），不要再依赖它。
2. **message_id 命名空间不通用**：SnowLuma 与 NapCat 各自生成消息 ID（哈希算法不同源）。本影子适配器只对**经 SnowLuma 收到的消息**有效。
3. **仅群消息支持贴表情**：SnowLuma 本体对私聊消息返回 `emoji reactions are not supported on private messages`。
4. **目标消息必须在服务端消息库中**：机器人只会对收到的消息贴表情，天然满足；手动传一个历史库外 ID 会得到 `message not found`。
5. **action 超时即重连**：echo 超时（默认 10s）视为链路异常，断开并重连，与官方适配器行为一致。
6. **入口开关的局限**：`register_napcat_*` 开关关闭后入口会拒绝服务，但 API 名仍注册在 Host 注册表中（装饰器静态注册）；极端的重名场景以「自动失效机制」兜底。
