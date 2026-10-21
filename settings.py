from typing import ClassVar

from maibot_sdk import Field, PluginConfigBase


class PluginSettings(PluginConfigBase):
    __ui_label__: ClassVar[str] = "插件"
    enabled: bool = Field(default=True, description="启用小黑盒社区适配器，可在侧边栏扩展页面扫码登录")
    config_version: str = "0.1.1"


class CommunitySettings(PluginConfigBase):
    __ui_label__: ClassVar[str] = "小黑盒社区"
    poll_interval: int = Field(default=60, ge=30, le=3600, description="通知轮询间隔（秒）")
    max_pages: int = Field(default=50, ge=1, le=500, description="单轮每类通知最大分页数，达到上限时报错并保留游标")
    reply_max_chars: int = Field(default=500, ge=1, le=2000, description="评论最大字数，超长回复直接报错")
    context_max_chars: int = Field(default=6000, ge=500, le=20000, description="原帖与楼层文字上下文长度")


class AdapterSettings(PluginConfigBase):
    plugin: PluginSettings = Field(default_factory=PluginSettings)
    community: CommunitySettings = Field(default_factory=CommunitySettings)
