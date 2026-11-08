import asyncio
import json
import time
import re
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set
from collections import defaultdict

from core.plugin import BasePlugin, logger, on, Priority, register_tool
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat import MessageChain
from core.chat.message_elements import Text, Image, At, File, Record, Video
from core.provider import LLMRequest
from core.agent.message import OpenAIMessage

from .utils.db import DatabaseManager
from .utils.render import HTMLRenderer
from .utils.avatar import AvatarCache


# 预定义暖色单色（用于活跃用户名称）
WARM_COLORS = [
    "#C0392B",  # 红
    "#D35400",  # 橙
    "#E67E22",  # 橙黄
    "#F39C12",  # 黄
    "#8E44AD",  # 紫
    "#2980B9",  # 蓝
    "#1ABC9C",  # 青
    "#27AE60",  # 绿
    "#6B4A3A",  # 棕
    "#E74C3C",  # 亮红
    "#F1C40F",  # 金黄
    "#2C3E50",  # 深蓝
]


def parse_user_tag(text: str):
    """从 "昵称#QQ号" 格式中提取昵称和 QQ号"""
    if '#' not in text:
        return text, None
    parts = text.rsplit('#', 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], parts[1]
    return text, None


def _persona_value(persona, key: str, default=None):
    """兼容 PersonaInfo（3.x）与 dict（2.x）两种人设对象。"""
    if persona is None:
        return default
    if isinstance(persona, dict):
        return persona.get(key, default)
    return getattr(persona, key, default)


# 第三方插件（如提醒插件）发布合成消息时使用的占位昵称：它们不是群成员的真实发言
SYNTHETIC_SENDERS = frozenset(
    {
        "提醒任务所有者",
        "system:reminder_plugin",
        "自主意图循环",
        "Web UI 用户",
        "Web UI 管理员",
    }
)


class KiraDailyReport(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)

        # ---- 辅助函数：从 section 中安全读取配置 ----
        def _get_from_section(section_name: str, key: str, default=None):
            section = cfg.get(section_name, {})
            return section.get(key, default)

        # ---- 基础设置 ----
        basic = cfg.get("section_basic", {})
        self.llm_model = basic.get("llm_model", "")
        self.theme = basic.get("theme", "warm")
        self.enabled_groups = basic.get("enabled_groups", [])
        self.auto_enabled_groups = basic.get("auto_enabled_groups", [])
        self.enable_command_trigger = basic.get("enable_command_trigger", False)
        self.enable_natural_language_trigger = basic.get("enable_natural_language_trigger", True)
        self.command_prefixes = basic.get("command_prefixes", ["/日报", "/群日报"])
        self.bot_nickname_override = basic.get("bot_nickname_override", "")
        self.adapter_name = basic.get("adapter_name", "")

        # ---- 分析参数 ----
        analysis = cfg.get("section_analysis", {})
        self.analysis_days = analysis.get("analysis_days", 1)
        try:
            self.max_messages_per_analysis = max(1, int(analysis.get("max_messages_per_analysis", 1000)))
        except (TypeError, ValueError):
            self.max_messages_per_analysis = 1000
        self.min_messages_threshold = analysis.get("min_messages_threshold", 10)
        self.auto_analysis_time = analysis.get("auto_analysis_time", "23:59")
        self.enable_auto_analysis = analysis.get("enable_auto_analysis", True)
        self.peak_window_minutes = analysis.get("peak_window_minutes", 90)

        # ---- 内容设置 ----
        content = cfg.get("section_content", {})
        self.enable_sharp_comment = content.get("enable_sharp_comment", True)
        self.inject_persona = content.get("inject_persona", True)
        self.persona_id = content.get("persona_id", "")
        self.topic_min = content.get("topic_min", 1)
        self.topic_max = content.get("topic_max", 5)
        self.quote_max = content.get("quote_max", 3)
        self.active_users_max = content.get("active_users_max", 10)

        # ---- 增量模式 ----
        incremental = cfg.get("section_incremental", {})
        self.enable_incremental_mode = incremental.get("enable_incremental_mode", False)
        self.incremental_group_list = incremental.get("incremental_group_list", [])
        self.enable_immediate_trigger = incremental.get("enable_immediate_trigger", True)
        self.incremental_interval_minutes = incremental.get("incremental_interval_minutes", 120)
        self.incremental_min_messages = incremental.get("incremental_min_messages", 10)
        self.window_hours = incremental.get("window_hours", 24)

        # ---- 权限与安全 ----
        permission = cfg.get("section_permission", {})
        self.cooldown_hours = permission.get("cooldown_hours", 4)
        self.whitelist_users = permission.get("whitelist_users", [])
        self.whitelist_exempt_cooldown = permission.get("whitelist_exempt_cooldown", True)
        self.max_concurrent_analysis = permission.get("max_concurrent_analysis", 3)
        self.exclude_senders = permission.get("exclude_senders", ["system"])

        # ---- 数据清理 ----
        cleanup = cfg.get("section_cleanup", {})
        self.message_retention_days = cleanup.get("message_retention_days", 15)
        self.batch_retention_days = cleanup.get("batch_retention_days", 7)
        self.report_retention_days = cleanup.get("report_retention_days", 7)
        self.report_cleanup_count = cleanup.get("report_cleanup_count", 7)

        # ---- 输出与渲染 ----
        output = cfg.get("section_output", {})
        self.output_format = output.get("output_format", "image")
        self.verbose_log = output.get("verbose_log", False)
        self.avatar_cache_expire_days = output.get("avatar_cache_expire_days", 3)
        self.prefer_system_browser = output.get("prefer_system_browser", True)
        self.render_timeout = output.get("render_timeout", 120)

        # ---- 自定义提示消息 ----
        messages = cfg.get("section_messages", {})
        self.msg_too_few = messages.get("msg_too_few", "📊 群聊日报：今日消息数 ({count}) 不足 {threshold} 条，暂不生成日报")
        self.msg_cooldown = messages.get("msg_cooldown", "⏳ 日报生成冷却中，剩余 {remaining:.1f} 小时")
        self.msg_not_enabled = messages.get("msg_not_enabled", "❌ 该群未启用日报功能")
        self.msg_not_group = messages.get("msg_not_group", "❌ 请在群聊中使用此功能")
        self.msg_processing = messages.get("msg_processing", "📊 正在生成日报，请稍候...")
        self.msg_failed = messages.get("msg_failed", "❌ 日报生成失败：{error}")
        self.msg_natural_disabled = messages.get("msg_natural_disabled", "ℹ️ 自然语言触发日报已禁用，请使用命令触发（如 /日报）")

        # ---- 存储路径 ----
        paths = cfg.get("section_paths", {})
        self.db_filename = paths.get("db_path", "messages.db")
        self.reports_dirname = paths.get("reports_dir", "reports")
        self.avatars_dirname = paths.get("avatar_cache_dir", "avatars")

        # ---- Bot 消息行为（内部固定） ----
        self._collect_bot_messages = True
        self._count_bot_in_stats = True
        self._include_bot_in_llm_analysis = False

        # ---- 初始化 ----
        self.data_dir: Path = ctx.get_plugin_data_dir()
        self.db_path = self.data_dir / self.db_filename
        self.reports_dir = self.data_dir / self.reports_dirname
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(str(self.db_path))
        self.renderer = HTMLRenderer(
            self.data_dir,
            prefer_system_browser=self.prefer_system_browser,
            render_timeout=self.render_timeout
        )
        self.avatar_cache = AvatarCache(
            self.data_dir / self.avatars_dirname,
            expire_days=self.avatar_cache_expire_days
        )

        self._semaphore = asyncio.Semaphore(self.max_concurrent_analysis)
        self._cooldown_map: Dict[str, float] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._incremental_scheduler_task: Optional[asyncio.Task] = None
        self._running = True

        self._bot_self_id: Optional[str] = None
        self._bot_nickname: Optional[str] = None
        self._bot_avatar: Optional[str] = None
        self._collected_bot_messages: Dict[str, None] = {}
        self._running_analysis_tasks: Set[str] = set()
        # 后台生成任务（按 sid 管理）：工具快速返回，生成完成后 publish_notice 通知 LLM 接话
        self._gen_tasks: Dict[str, asyncio.Task] = {}

        # 同群并发去重：定时/命令/工具/即时增量共用一个运行标记。
        # 语义是“已有任务在跑就跳过”，避免第二个触发排队几分钟后又生成一份重复日报。
        self._running_groups: Set[str] = set()
        # 即时增量触发的后台任务（不在 _gen_tasks 里，单独跟踪以便卸载时取消）
        self._analysis_tasks: Set[asyncio.Task] = set()
        # 多 QQ 适配器时按实例名缓存 bot 身份，避免 A 群的日报用 B 号的昵称/头像/self_id
        self._bot_infos: Dict[str, Dict[str, str]] = {}
        self._bot_info_locks: Dict[str, asyncio.Lock] = {}
        self._tzinfo = None
        self._tz_loaded = False

        logger.info("[KiraDaily] 插件已加载")

    # ============================================================
    # 生命周期
    # ============================================================

    async def initialize(self):
        await self.db.init()
        await self.db.create_tables()
        await self.avatar_cache.init()
        await self._fetch_bot_info()

        if not self.enable_natural_language_trigger:
            try:
                self.ctx.tool_mgr.unregister_tool("generate_daily_report")
                self._log("自然语言触发已禁用，已从LLM工具列表中移除 generate_daily_report")
            except Exception as e:
                self._log(f"注销工具失败（可能未注册）: {e}")
        else:
            self._log("自然语言触发已启用")

        # 旧配置里的适配器名失效时先迁移（原子写回）
        self._migrate_adapter_config()

        # 启动时清理过期数据
        await self._cleanup_old_reports()
        await self._cleanup_old_messages()
        await self._cleanup_synthetic_messages()

        if self.enable_auto_analysis:
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            logger.info(f"[KiraDaily] 定时任务已启动，每天 {self.auto_analysis_time} 执行")

        if self.enable_incremental_mode:
            self._incremental_scheduler_task = asyncio.create_task(self._incremental_scheduler_loop())
            logger.info(f"[KiraDaily] 增量分析已启动，间隔 {self.incremental_interval_minutes} 分钟")

        logger.info("[KiraDaily] 初始化完成")

    async def terminate(self):
        self._running = False
        # 先停调度器，确保它不会再派发新的分析任务；再取消已派发的任务，最后关库。
        for scheduler_task in (self._scheduler_task, self._incremental_scheduler_task):
            if scheduler_task and not scheduler_task.done():
                scheduler_task.cancel()
                try:
                    await scheduler_task
                except asyncio.CancelledError:
                    pass
        tasks = [t for t in list(self._gen_tasks.values()) + list(self._analysis_tasks) if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._gen_tasks.clear()
        self._analysis_tasks.clear()
        self._running_groups.clear()
        await self.db.close()
        logger.info("[KiraDaily] 已卸载")

    async def _cleanup_synthetic_messages(self):
        """清理历史上被第三方插件合成昵称污染的消息记录（保留真实发言）"""
        try:
            removed = await self.db.delete_messages_by_nicknames(
                sorted(SYNTHETIC_SENDERS)
            )
            if removed:
                logger.info(
                    f"[KiraDaily] 清理第三方插件合成消息 {removed} 条（不是群成员发言）"
                )
        except Exception as e:
            logger.warning(f"[KiraDaily] 清理合成消息失败: {e}")

    def _log(self, msg: str):
        if self.verbose_log:
            logger.info(f"[KiraDaily] {msg}")

    def _timezone(self):
        """跟随 KiraAI 的 locale.TZ；没有/无效时回退系统本地时区。

        配置时区在插件生命周期内不会变（变更需重载插件），首次读取后缓存，
        避免每条消息/每次格式化都去读配置。
        """
        if self._tz_loaded:
            return self._tzinfo
        getter = getattr(self.ctx, "get_timezone", None)
        self._tzinfo = None
        if callable(getter):
            try:
                self._tzinfo = getter()
            except Exception:
                self._tzinfo = None
        self._tz_loaded = True
        return self._tzinfo

    def _now(self):
        tz = self._timezone()
        return datetime.now(tz) if tz else datetime.now()

    def _dt(self, timestamp):
        tz = self._timezone()
        return datetime.fromtimestamp(timestamp, tz) if tz else datetime.fromtimestamp(timestamp)

    # ============================================================
    # 适配器解析：实例名可能是 QQ / 自定义名称，且查找区分大小写
    # ============================================================

    def _qq_adapters(self) -> Dict[str, object]:
        """当前已启用的 QQ 适配器（按实例名）。"""
        mgr = getattr(self.ctx, "adapter_mgr", None)
        if not mgr or not hasattr(mgr, "get_adapters"):
            return {}
        result = {}
        for name, adapter in (mgr.get_adapters() or {}).items():
            info = getattr(adapter, "info", None)
            platform = str(getattr(info, "platform", "") or "").strip().casefold()
            label = str(getattr(info, "name", "") or "").strip().casefold()
            if platform == "qq" or platform.startswith("qq") or label == "qq":
                result[name] = adapter
        return result

    def _resolve_adapter(self):
        """返回 (适配器实例, 实例名)。

        优先用配置里的实例名（先精确、再大小写不敏感）；留空或找不到时，
        自动取第一个已启用的 QQ 适配器，避免写死 "qq" 导致找不到。
        """
        mgr = getattr(self.ctx, "adapter_mgr", None)
        if not mgr or not hasattr(mgr, "get_adapter"):
            return None, ""
        configured = str(getattr(self, "adapter_name", "") or "").strip()
        if configured:
            adapter = mgr.get_adapter(configured)
            if adapter is not None:
                return adapter, configured
            lowered = configured.casefold()
            for name, adapter in (mgr.get_adapters() or {}).items():
                if name.casefold() == lowered:
                    return adapter, name
        qq = self._qq_adapters()
        if qq:
            name = next(iter(qq))
            return qq[name], name
        return None, ""

    @staticmethod
    def _session_adapter_name(event) -> str:
        session = getattr(event, "session", None)
        return str(getattr(session, "adapter_name", "") or "")

    def _migrate_adapter_config(self):
        """旧配置里的适配器实例名若已失效，原子清空以便自动探测（首次更新时执行）。"""
        configured = str(getattr(self, "adapter_name", "") or "").strip()
        if not configured:
            return
        mgr = getattr(self.ctx, "adapter_mgr", None)
        if not mgr or not hasattr(mgr, "get_adapter"):
            return
        if mgr.get_adapter(configured) is not None:
            return
        if not self._qq_adapters():
            # 适配器还没就绪时不要动配置，避免误清
            return
        try:
            import os

            from core.utils.path_utils import get_config_path

            plugin_id = Path(__file__).resolve().parent.name
            path = get_config_path() / "plugins" / f"{plugin_id}.json"
            if not path.exists():
                return
            config = json.loads(path.read_text(encoding="utf-8"))
            section = config.get("section_basic")
            if not isinstance(section, dict):
                return
            if str(section.get("adapter_name", "") or "").strip() != configured:
                return
            section["adapter_name"] = ""
            temporary = path.with_suffix(".adapter-migrate.tmp")
            temporary.write_text(
                json.dumps(config, ensure_ascii=False, indent=4), encoding="utf-8"
            )
            os.replace(temporary, path)  # 原子替换，失败不破坏原文件
            self.plugin_cfg = config
            try:
                self.ctx.plugin_mgr.plugin_configs[plugin_id] = config
            except Exception:
                pass
            self.adapter_name = ""
            logger.info(
                f"[KiraDaily] 旧适配器名 {configured!r} 已失效，已清空并改为自动探测第一个 QQ 适配器"
            )
        except Exception as e:
            logger.warning(f"[KiraDaily] 适配器配置迁移失败（保持原值）: {e}")

    # ============================================================
    # 获取 Bot 信息
    # ============================================================

    def _group_adapter_name(self, group_id: str) -> str:
        parts = str(group_id or "").split(":")
        return parts[0] if len(parts) >= 3 else ""

    def _adapter_for_group(self, group_id: str):
        """返回本群应使用的适配器。

        兼容旧配置：显式填写了有效的「QQ 适配器实例名」时，继续全程使用该实例
        （与旧版行为一致）；留空/失效时才按群 ID 前缀自动识别，最后回退全局探测。
        """
        mgr = getattr(self.ctx, "adapter_mgr", None)
        configured = str(getattr(self, "adapter_name", "") or "").strip()
        if configured and mgr and hasattr(mgr, "get_adapter"):
            try:
                adapter = mgr.get_adapter(configured)
                if adapter is None:
                    lowered = configured.casefold()
                    for name, candidate in (mgr.get_adapters() or {}).items():
                        if name.casefold() == lowered:
                            adapter = candidate
                            configured = name
                            break
                if adapter is not None:
                    return adapter, configured
            except Exception:
                pass

        name = self._group_adapter_name(group_id)
        if name and mgr and hasattr(mgr, "get_adapter"):
            try:
                adapter = mgr.get_adapter(name)
                if adapter is not None:
                    info = getattr(adapter, "info", None)
                    platform = str(getattr(info, "platform", "") or "").strip().casefold()
                    label = str(getattr(info, "name", "") or "").strip().casefold()
                    # 只按群所属的 QQ 适配器取 bot 身份；非 QQ 群保持原来的全局探测行为
                    if platform == "qq" or platform.startswith("qq") or label == "qq":
                        return adapter, name
            except Exception:
                pass
        return self._resolve_adapter()

    async def _load_bot_info(self, adapter) -> Dict[str, str]:
        """读取单个适配器的 bot 身份；不写实例字段，供多适配器并发安全使用。"""
        self_id = ""
        nickname = self.bot_nickname_override or ""
        avatar = ""
        try:
            if adapter is not None:
                info = getattr(adapter, "info", None)
                conf = getattr(info, "config", None) or {}
                self_id = str(conf.get("self_id", "") or "")
                if not self_id:
                    bot = getattr(adapter, "bot", None)
                    if bot is not None and hasattr(bot, "uin"):
                        self_id = str(bot.uin or "")
                    elif bot is not None and hasattr(bot, "self_id"):
                        self_id = str(bot.self_id or "")

                if self_id:
                    avatar = await self.avatar_cache.get_bot_avatar(self_id)
                    if not nickname and hasattr(adapter, "get_login_info"):
                        try:
                            login = await adapter.get_login_info()
                            if login and login.get("nickname"):
                                nickname = login["nickname"]
                        except Exception:
                            pass
                    if not nickname:
                        bot = getattr(adapter, "bot", None)
                        if bot is not None and hasattr(bot, "send_action"):
                            try:
                                result = await bot.send_action("get_login_info", {})
                                if result and result.get("status") == "ok":
                                    data = result.get("data", {})
                                    if data.get("nickname"):
                                        nickname = data["nickname"]
                            except Exception:
                                pass
            if not self_id:
                self_id = str(self.ctx.config.get("bot_config.bot.self_id", "") or "")
                if self_id:
                    avatar = await self.avatar_cache.get_bot_avatar(self_id)
            if not self_id:
                self_id = "0"
            if not avatar:
                avatar = await self.avatar_cache.get_bot_avatar(self_id)
        except Exception as e:
            self._log(f"获取 bot 信息失败: {e}")
        if not self_id:
            self_id = "0"
        if not nickname:
            nickname = "AI"
        if not avatar:
            try:
                avatar = await self.avatar_cache.get_bot_avatar(self_id or "0")
            except Exception:
                avatar = ""
        return {"self_id": self_id, "nickname": nickname, "avatar": avatar}

    async def _get_bot_info(self, adapter, adapter_name: str = "", force: bool = False) -> Dict[str, str]:
        """按适配器缓存 bot 身份；并发请求同一适配器只拉一次。"""
        key = adapter_name or ""
        if not force and key in self._bot_infos:
            return self._bot_infos[key]
        lock = self._bot_info_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._bot_info_locks[key] = lock
        async with lock:
            if not force and key in self._bot_infos:
                return self._bot_infos[key]
            info = await self._load_bot_info(adapter)
            # 头像失败时不要永久缓存空值，下一次日报会重试。
            if info.get("avatar") and (force or info.get("self_id") != "0" or not key):
                self._bot_infos[key] = info
            return info

    async def _resolve_persona(self):
        """空 = 当前激活人设；非空 = 人设 ID（下拉框存的就是 ID）；无效 ID 回退当前激活人设。"""
        if not self.inject_persona:
            return None
        mgr = getattr(self.ctx, "persona_mgr", None)
        if mgr is None:
            return None

        raw = str(getattr(self, "persona_id", "") or "").strip()
        if raw:
            try:
                persona = await mgr.get_persona(raw)
            except Exception:
                persona = None
            if persona:
                return persona
            # 无效 ID（例如人设已删除）按“未选择”处理，回退当前激活人设。
        try:
            return await mgr.get_active_persona()
        except Exception:
            return None

    async def _fetch_bot_info(self):
        try:
            adapter, adapter_name = self._resolve_adapter()
            info = await self._get_bot_info(adapter, adapter_name, force=True)
            self._bot_self_id = info["self_id"]
            self._bot_nickname = info["nickname"]
            self._bot_avatar = info["avatar"]
        except Exception as e:
            self._log(f"获取 bot 信息失败: {e}")
            self._bot_self_id = "0"
            self._bot_nickname = self.bot_nickname_override or "AI"
            try:
                self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")
            except Exception:
                self._bot_avatar = ""

    # ============================================================
    # 获取群名称
    # ============================================================

    async def _get_group_name(self, group_id: str) -> str:
        try:
            adapter, _adapter_name = self._adapter_for_group(group_id)
            if not adapter:
                return group_id
            parts = group_id.split(":")
            if len(parts) >= 3:
                qq_group_id = parts[2]
            else:
                qq_group_id = group_id
            if hasattr(adapter, "bot") and hasattr(adapter.bot, "send_action"):
                result = await adapter.bot.send_action("get_group_info", {"group_id": int(qq_group_id)})
                if result and result.get("status") == "ok":
                    data = result.get("data", {})
                    return data.get("group_name", group_id)
            return group_id
        except Exception as e:
            self._log(f"获取群名称失败: {e}")
            return group_id

    # ============================================================
    # 群组权限检查
    # ============================================================

    def _is_group_enabled(self, group_id: str) -> bool:
        if not self.enabled_groups:
            return True
        return group_id in self.enabled_groups

    def _is_auto_enabled(self, group_id: str) -> bool:
        if not self.auto_enabled_groups:
            return self._is_group_enabled(group_id)
        return group_id in self.auto_enabled_groups

    def _is_incremental_group(self, group_id: str) -> bool:
        if not self.enable_incremental_mode:
            return False
        if not self.incremental_group_list:
            return True
        return group_id in self.incremental_group_list

    def _get_group_id_from_event(self, event) -> Optional[str]:
        try:
            if hasattr(event, "sid"):
                sid = str(event.sid)
                parts = sid.split(":")
                if len(parts) == 3 and parts[1] == "gm" and all(parts):
                    return sid
                return None
            if hasattr(event, "message") and hasattr(event.message, "group"):
                if event.message.group:
                    name = self._session_adapter_name(event)
                    if not name:
                        _adapter, name = self._resolve_adapter()
                    if name:
                        return f"{name}:gm:{event.message.group.group_id}"
        except Exception:
            pass
        return None

    def _get_user_id_from_event(self, event) -> str:
        try:
            if hasattr(event, "message") and hasattr(event.message, "sender"):
                return str(event.message.sender.user_id)
            elif hasattr(event, "messages") and event.messages:
                return str(event.messages[0].sender.user_id)
        except Exception:
            pass
        return "unknown"

    def _get_sid(self, event) -> str:
        if hasattr(event, "sid"):
            return event.sid
        if hasattr(event, "session") and hasattr(event.session, "sid"):
            return event.session.sid
        return "default"

    # ============================================================
    # 直接发送消息到群
    # ============================================================

    async def _send_text_to_group(self, group_id: str, text: str):
        try:
            adapter, _adapter_name = self._adapter_for_group(group_id)
            if not adapter:
                return
            parts = group_id.split(":")
            if len(parts) >= 3:
                qq_group_id = parts[2]
            else:
                qq_group_id = group_id
            await self._send_group_msg(adapter, qq_group_id, MessageChain([Text(text)]))
        except Exception as e:
            self._log(f"发送文本到群 {group_id} 失败: {e}")

    async def _send_group_msg(self, adapter, group_id: str, chain):
        """群发送（双世代，特征识别不绑版本号）。
        3.0 优先走 IMCapability 能力对象（官方契约路径）；
        2.x 与 3.0 的 QQ 适配器委托方法回退。"""
        try:
            from core.adapter import IMCapability  # 仅 3.0 能力架构存在（2.x 无此符号）
        except ImportError:
            IMCapability = None
        try:
            get_cap = getattr(adapter, "get_capability", None)
            if IMCapability is not None and get_cap is not None:
                im = get_cap(IMCapability)
                if im is not None and hasattr(im, "send_group_message"):
                    return await im.send_group_message(group_id, chain)
        except Exception:
            pass
        return await adapter.send_group_message(group_id, chain)

    async def _publish_notice(self, sid: str, text: str) -> None:
        """publish_notice：构造合成事件进主线路，LLM 接话完成回复（消息合并进会话）。

        用于后台生成任务完成后，让 LLM 补一句"日报已生成"并自然合并消息。
        """
        try:
            await self.ctx.publish_notice(sid, MessageChain([Text(text)]), is_mentioned=True)
        except Exception:
            logger.exception("[KiraDaily] publish_notice failed")

    def _report_files(self) -> Set[str]:
        """当前报告目录下的文件名集合（用于判断后台任务是否真正生成了新报告）。"""
        try:
            if self.reports_dir.exists():
                return {p.name for p in self.reports_dir.glob("*")}
        except Exception:
            pass
        return set()

    # ============================================================
    # 冷却检查
    # ============================================================

    def _is_in_cooldown(self, group_id: str, user_id: str) -> bool:
        if self.cooldown_hours <= 0:
            return False
        if self.whitelist_exempt_cooldown and user_id in self.whitelist_users:
            return False
        if user_id == "system_immediate":
            return False
        last_run = self._cooldown_map.get(group_id, 0)
        if last_run == 0:
            return False
        elapsed = time.time() - last_run
        return elapsed < self.cooldown_hours * 3600

    def _update_cooldown(self, group_id: str):
        self._cooldown_map[group_id] = time.time()

    # ============================================================
    # 消息收集（普通用户 + Bot）
    # ============================================================

    @on.im_message(priority=Priority.HIGH)
    async def collect_message(self, event: KiraMessageEvent):
        if not event.is_group_message():
            return

        group_id = self._get_group_id_from_event(event)
        if not group_id or not self._is_group_enabled(group_id):
            return

        if getattr(event.message, "is_notice", False):
            self._log("忽略第三方插件的合成通知消息，不计入日报")
            return

        sender_nickname = event.message.sender.nickname if event.message.sender else ""
        if sender_nickname in SYNTHETIC_SENDERS:
            self._log(f"忽略合成昵称 {sender_nickname}（第三方插件），不计入日报")
            return
        if sender_nickname in self.exclude_senders:
            self._log(f"屏蔽消息: 发送者 {sender_nickname} 在排除列表中，已忽略")
            return

        self_id = str(event.message.self_id) if hasattr(event.message, 'self_id') else None
        user_id = str(event.message.sender.user_id) if event.message.sender else None
        if self_id and user_id == self_id:
            if not self._collect_bot_messages:
                self._log(f"忽略 bot 自己的消息 (收集开关已关闭)")
                return

        text_parts = []
        for elem in event.message.chain:
            if isinstance(elem, Text):
                text_parts.append(elem.text)
            elif isinstance(elem, At):
                text_parts.append(f"@{elem.nickname or elem.pid}")
            elif isinstance(elem, Image):
                text_parts.append("[图片]")
            elif isinstance(elem, (File, Record, Video)):
                text_parts.append(f"[{elem.__class__.__name__}]")

        content = " ".join(text_parts).strip()
        if not content:
            content = "[图片/表情]"

        user_id = str(event.message.sender.user_id)
        nickname = event.message.sender.nickname or "未知"
        timestamp = event.message.timestamp or int(time.time())

        await self.db.save_message(
            session_id=group_id,
            user_id=user_id,
            nickname=nickname,
            content=content,
            timestamp=timestamp
        )
        self._log(f"收集消息: {group_id} | {nickname}: {content[:30]}...")

        # 增量模式实时触发检测
        if (self.enable_incremental_mode and
            self.enable_immediate_trigger and
            self._is_incremental_group(group_id)):

            last_time = await self.db.get_last_incremental_time(group_id)
            if last_time == 0:
                return

            new_msgs = await self.db.get_messages_since(group_id, last_time)
            if len(new_msgs) >= self.max_messages_per_analysis:
                task_key = f"incremental_{group_id}"
                if task_key not in self._running_analysis_tasks:
                    if not self._is_in_cooldown(group_id, "system_immediate"):
                        self._log(f"⚡ 触发实时分析：群 {group_id} 新增消息已达 {len(new_msgs)} 条")
                        self._running_analysis_tasks.add(task_key)
                        task = asyncio.create_task(
                            self._safe_do_analysis(group_id, "system_immediate", immediate=True)
                        )
                        self._analysis_tasks.add(task)
                        task.add_done_callback(self._analysis_tasks.discard)

    # ============================================================
    # 备选：通过 message_sent 捕获 Bot 消息
    # ============================================================

    @on.message_sent(priority=Priority.MEDIUM)
    async def collect_bot_message(self, event: KiraMessageBatchEvent, *args, **kwargs):
        if not self._collect_bot_messages:
            return

        if not event.messages:
            return
        last_msg = event.messages[-1]

        group_id = None
        name = self._session_adapter_name(event)
        if not name:
            _adapter, name = self._resolve_adapter()
        if hasattr(last_msg, 'group') and last_msg.group and name:
            group_id = f"{name}:gm:{last_msg.group.group_id}"
        else:
            return

        if not group_id or not self._is_group_enabled(group_id):
            return

        user_id = str(last_msg.sender.user_id) if last_msg.sender else None
        self_id = str(event.self_id) if hasattr(event, 'self_id') else None

        if not self_id or user_id != self_id:
            return

        msg_id = str(last_msg.message_id) if hasattr(last_msg, 'message_id') else None
        if msg_id:
            # 按 适配器+消息ID 去重：不同适配器的消息 ID 可能撞号
            dedup_key = f"{name}:{msg_id}"
            if dedup_key in self._collected_bot_messages:
                return
            self._collected_bot_messages[dedup_key] = None
            if len(self._collected_bot_messages) > 5000:
                oldest = next(iter(self._collected_bot_messages))
                self._collected_bot_messages.pop(oldest, None)

        text_parts = []
        has_valid_content = False
        for elem in last_msg.chain:
            if isinstance(elem, (Text, At, Image, File, Record, Video)):
                has_valid_content = True
                if isinstance(elem, Text):
                    text_parts.append(elem.text)
                elif isinstance(elem, At):
                    text_parts.append(f"@{elem.nickname or elem.pid}")
                elif isinstance(elem, Image):
                    text_parts.append("[图片]")
                elif isinstance(elem, (File, Record, Video)):
                    text_parts.append(f"[{elem.__class__.__name__}]")
        if not has_valid_content:
            return

        content = " ".join(text_parts).strip()
        if not content:
            content = "[图片/表情]"

        nickname = last_msg.sender.nickname if last_msg.sender else "Bot"
        if nickname == "未知" or nickname == "unknown":
            bot_info = await self._get_bot_info(*self._adapter_for_group(group_id))
            nickname = bot_info.get("nickname") or self._bot_nickname or "Bot"
        timestamp = last_msg.timestamp or int(time.time())

        await self.db.save_message(
            session_id=group_id,
            user_id=user_id,
            nickname=nickname,
            content=content,
            timestamp=timestamp
        )
        self._log(f"收集bot消息(备用): {group_id} | {nickname}: {content[:30]}...")

    # ============================================================
    # LLM 调用
    # ============================================================

    async def _call_llm(self, prompt: str) -> dict:
        if self.llm_model:
            client = self.ctx.get_llm_client(model_uuid=self.llm_model)
            if client is None:
                logger.warning(f"指定的 LLM 模型 {self.llm_model} 不存在，回退到快速模型")
                client = self.ctx.get_default_fast_llm_client()
        else:
            client = self.ctx.get_default_fast_llm_client()

        if client is None:
            raise RuntimeError("没有可用的 LLM 客户端")

        req = LLMRequest(messages=[OpenAIMessage(role="user", content=prompt)])
        resp = await client.chat(req)

        text = resp.text_response.strip()
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {}

    # ============================================================
    # ⭐ 活跃时段精确统计算法（改进版：使用密度比判断）
    # ============================================================

    def _calculate_peak_hour(self, messages: List[dict]) -> str:
        """
        基于消息时间戳，计算最活跃的时间段。
        使用密度比（峰值密度 vs 平均密度）判断是否有显著峰值。
        如果密度比 >= 1.5，输出峰值窗口；否则回退到最近窗口内的消息范围。
        """
        if not messages:
            return "暂无消息"

        timestamps = sorted([msg['timestamp'] for msg in messages])
        n = len(timestamps)

        if n == 1:
            dt = self._dt(timestamps[0])
            return f"{dt.strftime('%H:%M')}"

        if n == 2:
            start_dt = self._dt(timestamps[0])
            end_dt = self._dt(timestamps[1])
            if start_dt.date() == end_dt.date():
                return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
            else:
                return f"{start_dt.strftime('%m-%d %H:%M')}-{end_dt.strftime('%m-%d %H:%M')}"

        window_seconds = self.peak_window_minutes * 60
        total_span = timestamps[-1] - timestamps[0]

        # ---- 滑动窗口找消息数最多的窗口 ----
        best_start_idx = 0
        best_end_idx = 0
        best_count = 0

        j = 0
        for i in range(n):
            if j < i:
                j = i
            while j + 1 < n and timestamps[j + 1] - timestamps[i] <= window_seconds:
                j += 1
            count = j - i + 1
            if count > best_count:
                best_count = count
                best_start_idx = i
                best_end_idx = j

        # ---- 计算密度比，判断是否有显著峰值 ----
        window_span = timestamps[best_end_idx] - timestamps[best_start_idx]
        if window_span <= 0:
            window_span = 60

        avg_density = n / max(total_span, 1)
        peak_density = best_count / window_span
        density_ratio = peak_density / avg_density if avg_density > 0 else 1.0

        # 密度比 < 1.5：无显著峰值，回退到最近窗口内的消息
        if density_ratio < 1.5:
            cutoff_ts = timestamps[-1] - window_seconds
            start_idx = 0
            for idx in range(n):
                if timestamps[idx] >= cutoff_ts:
                    start_idx = idx
                    break
            # 如果窗口内消息少于3条，取最近3条
            if n - start_idx < 3:
                start_idx = max(0, n - 3)
            start_ts = timestamps[start_idx]
            end_ts = timestamps[-1]

            start_dt = self._dt(start_ts)
            end_dt = self._dt(end_ts)
            if start_dt.date() == end_dt.date():
                return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
            else:
                return f"{start_dt.strftime('%m-%d %H:%M')}-{end_dt.strftime('%m-%d %H:%M')}"

        # ---- 有明显峰值，进一步收缩窗口 ----
        sub_window = window_seconds * 0.5
        sub_best_start = best_start_idx
        sub_best_end = best_end_idx
        sub_best_density = 0

        sub_j = best_start_idx
        for i in range(best_start_idx, best_end_idx + 1):
            if sub_j < i:
                sub_j = i
            while sub_j + 1 <= best_end_idx and timestamps[sub_j + 1] - timestamps[i] <= sub_window:
                sub_j += 1
            count = sub_j - i + 1
            if count >= 3:
                sub_span = timestamps[sub_j] - timestamps[i]
                if sub_span <= 0:
                    sub_span = 60
                density = count / sub_span
                if density > sub_best_density:
                    sub_best_density = density
                    sub_best_start = i
                    sub_best_end = sub_j

        if sub_best_density > 0 and sub_best_density > peak_density * 0.8:
            start_ts = timestamps[sub_best_start]
            end_ts = timestamps[sub_best_end]
        else:
            start_ts = timestamps[best_start_idx]
            end_ts = timestamps[best_end_idx]

        start_dt = self._dt(start_ts)
        end_dt = self._dt(end_ts)

        if start_dt.date() == end_dt.date():
            return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
        else:
            return f"{start_dt.strftime('%m-%d %H:%M')}-{end_dt.strftime('%m-%d %H:%M')}"

    # ============================================================
    # ⭐ 统一润色方法（合并 3 次调用为 1 次）
    # ============================================================

    async def _generate_full_daily_with_llm(
        self,
        topic_candidates: List[dict],
        quote_candidates: List[dict],
        active_users: List[dict],
        existing_sharp_comments: List[str] = None
    ) -> Optional[dict]:
        """
        一次性让 LLM 完成所有润色任务：
        1. 话题精选 + 精炼（topic_max 条）
        2. 金句精选（quote_max 条）
        3. 最佳金句评选
        4. header_comment（一句话总结）
        5. sharp_comment（AI锐评）
        """
        # 构建候选池文本
        topic_text = "\n".join([
            f"{i+1}. {t.get('title', '')}：{t.get('detail', '')}"
            for i, t in enumerate(topic_candidates[:20])
        ]) if topic_candidates else "无候选话题"

        quote_text = "\n".join([
            f"{i+1}. {q.get('text', '')} —— {q.get('sender', '未知')}"
            for i, q in enumerate(quote_candidates[:20])
        ]) if quote_candidates else "无候选金句"

        user_text = "、".join([u.get('name', '') for u in active_users[:5]]) if active_users else "暂无活跃用户"

        sharp_ref = ""
        if existing_sharp_comments:
            sharp_ref = f"\n历史锐评风格参考（仅作风格参考，不要照抄）：{existing_sharp_comments[-1]}"

        # 构建人设风格提示（本站只修“选谁”，提示词原文不动）
        persona_prompt = ""
        if self.inject_persona:
            persona = await self._resolve_persona()
            content = str(_persona_value(persona, "content", "") or "")
            if content.strip():
                persona_prompt = f"\n请以以下人设风格进行点评和吐槽：\n{content}\n"

        prompt = f"""你是一个群聊日报精炼师。请根据以下群聊汇总信息，完成五项任务。

{persona_prompt}

【候选话题】（需选出 {self.topic_max} 个最具代表性的，可合并/修改标题/补充描述）：
{topic_text}

【候选金句】（需选出 {self.quote_max} 条最精彩的）：
{quote_text}

【活跃用户】：{user_text}
{sharp_ref}

【任务】
1. 话题精选：从候选话题中选出 {self.topic_max} 个最具代表性的。如果两个话题相似，合并成一个更完整的话题，并更新标题和描述。每个话题附带一句 AI 吐槽。
2. 金句精选：从候选金句中选出 {self.quote_max} 条最精彩的。
3. 最佳金句：从选出的金句中评选一条为"最佳金句"，给出精炼的评选理由（不超过15字）。
4. 一句话总结：用一句简短有趣的话概括今日群聊氛围，不超过20字，不带表情符号。
5. AI锐评：用一句毒舌/幽默的话锐评今日群聊，不超过30字。

【输出格式】（严格 JSON）：
{{
  "topics": [
    {{"title": "话题标题", "detail": "话题描述", "roast": "AI吐槽"}}
  ],
  "quotes": [
    {{"text": "金句原文", "sender": "发送者（保持 #QQ号 格式）"}}
  ],
  "best_quote": {{
    "text": "最佳金句原文",
    "sender": "发送者",
    "reason": "评选理由"
  }},
  "header_comment": "一句话总结",
  "sharp_comment": "AI锐评"
}}
"""

        try:
            result = await self._call_llm(prompt)
            if isinstance(result, dict):
                return result
        except Exception as e:
            self._log(f"LLM 统一生成失败: {e}")

        return None

    # ============================================================
    # 全量分析 Prompt
    # ============================================================

    async def _build_analysis_prompt(
        self,
        messages: List[dict],
        truncated: bool,
        bot_self_id: Optional[str] = None,
    ) -> str:
        self_id = bot_self_id or self._bot_self_id
        if self_id:
            messages = [m for m in messages if m['user_id'] != self_id]

        msg_lines = []
        limit = max(1, int(self.max_messages_per_analysis or 1000))
        for msg in messages[-limit:]:
            ts = self._dt(msg["timestamp"]).strftime("%H:%M")
            user_tag = f"{msg['nickname']}#{msg['user_id']}"
            msg_lines.append(f"[{ts}] {user_tag}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        # 人设是“谁在写”，不是“用什么风格写”：有可用人设时把 content 放在最前面，
        # 让总结/话题/锐评都从这个人设出发；没有时才退回原来的分析师写法。
        persona_block = "你是一个群聊数据分析师。请分析以下群聊记录，生成一份简洁的日报。\n\n"
        if self.inject_persona:
            persona = await self._resolve_persona()
            content = str(_persona_value(persona, "content", "") or "")
            if content.strip():
                persona_block = (
                    f"{content}\n\n"
                    "以上是你的身份、性格和说话方式，按以上人设风格进行分析和点评，生成一份简洁的日报："
                )

        sharp_instruction = """
6. 锐评：用一句话对今天的群聊进行锐评（毒舌、幽默或温柔），语气符合人设风格。
""" if self.enable_sharp_comment else ""

        prompt = f"""{persona_block}

【重要：用户标识】
聊天记录中每个用户名字后面都有 `#QQ号` 后缀（如 `周武#123456`），即使名字相同，QQ号不同代表不同用户。请在输出中保持使用完整的 `名字#QQ号` 格式。

【分析要求】
1. 一句话总结：用一句简短有趣的话概括今天群聊的氛围（放在日报头部）。
2. 统计数据：总消息数、参与人数、最活跃时段（请给出时间段，格式如 "20:00-22:00"）。
3. 热门话题：提取{self.topic_min}-{self.topic_max}个主要话题，每个话题包含：标题、描述、AI吐槽（一句话毒舌吐槽）。
4. 活跃用户：列出发言最多的前{self.active_users_max}位用户及发言数，`name` 字段必须使用 `名字#QQ号` 格式。
5. 金句：选出{self.quote_max}条最精彩的发言。如果有多条，标注其中一条为"最佳金句"并说明理由，`sender` 字段必须使用 `名字#QQ号` 格式。
{sharp_instruction}

【输出格式】（严格按JSON格式输出）
{{
  "header_comment": "一句话概括今日群聊",
  "stats": {{"total_messages": 0, "participants": 0, "peak_hour": "20:00-22:00"}},
  "topics": [
    {{"title": "话题1", "detail": "简短描述", "roast": "AI吐槽"}}
  ],
  "active_users": [
    {{"name": "周武#123456", "count": 10}}
  ],
  "quotes": [
    {{"text": "金句内容", "sender": "周武#123456"}}
  ],
  "best_quote": {{
    "text": "最佳金句内容",
    "sender": "周武#123456",
    "reason": "AI选取此句的理由"
  }},
  "sharp_comment": "AI锐评一句话"
}}

【聊天记录】
{msg_text}
"""
        return prompt

    # ============================================================
    # 增量分析 Prompt
    # ============================================================

    async def _call_llm_incremental(self, messages: List[dict], bot_self_id: Optional[str] = None) -> dict:
        self_id = bot_self_id or self._bot_self_id
        if self_id:
            messages = [m for m in messages if m['user_id'] != self_id]

        msg_lines = []
        limit = max(1, int(self.max_messages_per_analysis or 1000))
        for msg in messages[-limit:]:
            ts = self._dt(msg["timestamp"]).strftime("%H:%M")
            user_tag = f"{msg['nickname']}#{msg['user_id']}"
            msg_lines.append(f"[{ts}] {user_tag}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        prompt = f"""请分析以下聊天记录，提取关键信息。

【重要：用户标识】
聊天记录中每个用户名字后面都有 `#QQ号` 后缀（如 `周武#123456`），即使名字相同，QQ号不同代表不同用户。请在输出中保持使用完整的 `名字#QQ号` 格式。

【要求】
1. 提取 2-3 个话题（标题+简短描述+AI吐槽）
2. 提取 2-3 条金句（发言内容+发送者，`sender` 字段必须使用 `名字#QQ号` 格式）
3. 一句话锐评

【输出格式】（严格JSON）
{{
  "topics": [{{"title": "...", "detail": "...", "roast": "..."}}],
  "quotes": [{{"text": "...", "sender": "周武#123456"}}],
  "sharp_comment": "..."
}}

【聊天记录】
{msg_text}
"""
        return await self._call_llm(prompt)

    # ============================================================
    # 增量分析核心（修复：无新消息/不足阈值视为正常，不触发回退）
    # ============================================================

    async def _do_incremental_analysis(self, group_id: str) -> bool:
        """
        执行增量分析，返回 True 表示处理成功（可能无新消息或已分析），
        False 表示出现错误（如 LLM 调用失败）需要回退到全量。
        """
        try:
            last_time = await self.db.get_last_incremental_time(group_id)
            if last_time == 0:
                last_time = int(time.time()) - self.window_hours * 3600

            messages = await self.db.get_messages_since(group_id, last_time)
            if not messages:
                self._log(f"增量分析跳过：群 {group_id} 无新增消息")
                return True

            if len(messages) < self.incremental_min_messages:
                self._log(f"增量分析跳过：群 {group_id} 新增消息不足 {self.incremental_min_messages} 条")
                await self.db.update_last_incremental_time(group_id, messages[-1]['timestamp'])
                return True

            if len(messages) > self.max_messages_per_analysis:
                messages = messages[-self.max_messages_per_analysis:]

            bot_info = await self._get_bot_info(*self._adapter_for_group(group_id))
            bot_self_id = bot_info.get("self_id", "")
            result = await self._call_llm_incremental(messages, bot_self_id=bot_self_id)
            if not result:
                self._log(f"增量分析 LLM 返回空结果：群 {group_id}")
                return False

            quotes = result.get('quotes', [])
            filtered_quotes = []
            for q in quotes:
                sender = q.get('sender', '')
                _, uid = parse_user_tag(sender)
                if uid and uid == bot_self_id:
                    continue
                filtered_quotes.append(q)
            result['quotes'] = filtered_quotes

            await self.db.save_incremental_batch(
                session_id=group_id,
                start_timestamp=last_time,
                end_timestamp=messages[-1]['timestamp'],
                message_count=len(messages),
                participants=len(set(m['user_id'] for m in messages)),
                topics=result.get('topics', []),
                quotes=result.get('quotes', []),
                active_users=[],
                sharp_comment=result.get('sharp_comment', '')
            )

            await self.db.update_last_incremental_time(group_id, messages[-1]['timestamp'])

            await self._merge_with_cumulative(group_id)

            self._log(f"增量分析成功：群 {group_id}，{len(messages)} 条消息，已合并到累积结果")
            return True
        except Exception as e:
            logger.error(f"[KiraDaily] 增量分析失败 {group_id}: {e}")
            return False

    # ============================================================
    # 累积结果管理
    # ============================================================

    async def _get_cumulative(self, group_id: str) -> Optional[dict]:
        return await self.db.get_cumulative_result(group_id)

    async def _save_cumulative(
        self,
        group_id: str,
        topics: List[dict],
        quotes: List[dict],
        active_users: List[dict],
        topic_counts: dict,
        user_counts: dict,
        total_messages: int,
        participants: int,
        last_batch_timestamp: int,
        merged_batch_count: int
    ):
        await self.db.save_cumulative_result(
            group_id, topics, quotes, active_users, topic_counts,
            user_counts, total_messages, participants,
            last_batch_timestamp, merged_batch_count
        )

    async def _delete_cumulative(self, group_id: str):
        await self.db.delete_cumulative_result(group_id)

    async def _is_cross_day(self, group_id: str) -> bool:
        cumulative = await self._get_cumulative(group_id)
        if not cumulative:
            return True
        last_ts = cumulative.get('last_batch_timestamp', 0)
        if last_ts == 0:
            return True
        return (time.time() - last_ts) > self.window_hours * 3600

    async def _build_cumulative_from_batches(self, group_id: str) -> bool:
        cutoff = int(time.time()) - self.window_hours * 3600
        batches = await self.db.get_incremental_batches(group_id, cutoff)
        if not batches:
            await self._delete_cumulative(group_id)
            self._log(f"重建累积结果：群 {group_id} 窗口内无批次，已删除累积结果")
            return False

        topic_counts: Dict[str, int] = {}
        quote_counts: Dict[str, int] = {}
        topic_meta: Dict[str, dict] = {}
        quote_meta: Dict[str, dict] = {}
        user_counts: Dict[str, int] = {}
        total_messages = 0
        last_batch_ts = 0

        for batch in batches:
            try:
                topics = json.loads(batch['topics_json'])
                quotes = json.loads(batch['quotes_json'])

                total_messages += batch.get('message_count', 0)
                last_batch_ts = max(last_batch_ts, batch.get('end_timestamp', 0))

                for topic in topics:
                    title = topic.get('title', '')
                    if title:
                        topic_counts[title] = topic_counts.get(title, 0) + 1
                        if title not in topic_meta:
                            topic_meta[title] = topic

                for quote in quotes:
                    text = quote.get('text', '')
                    if text:
                        quote_counts[text] = quote_counts.get(text, 0) + 1
                        if text not in quote_meta:
                            quote_meta[text] = quote

                users = json.loads(batch['active_users_json'])
                for user in users:
                    name = user.get('name', '')
                    if name:
                        user_counts[name] = user_counts.get(name, 0) + user.get('count', 0)

            except Exception as e:
                self._log(f"解析批次失败: {e}")

        if not topic_meta and not quote_meta and not user_counts:
            self._log(f"重建累积结果：群 {group_id} 无有效数据")
            return False

        # 话题候选池
        topic_candidates = list(topic_meta.values())
        # 金句候选池
        quote_candidates = list(quote_meta.values())

        # 活跃用户
        sorted_users = sorted(
            [{"name": k, "count": v} for k, v in user_counts.items()],
            key=lambda x: x.get('count', 0),
            reverse=True
        )[:self.active_users_max]

        # 统一调用 LLM 生成
        result = await self._generate_full_daily_with_llm(
            topic_candidates=topic_candidates,
            quote_candidates=quote_candidates,
            active_users=sorted_users,
            existing_sharp_comments=[]
        )

        if result:
            topics = result.get('topics', [])[:self.topic_max]
            quotes = result.get('quotes', [])[:self.quote_max]
        else:
            topics = topic_candidates[:self.topic_max]
            quotes = quote_candidates[:self.quote_max]

        await self._save_cumulative(
            group_id,
            topics,
            quotes,
            sorted_users,
            topic_counts,
            user_counts,
            total_messages,
            len(user_counts),
            last_batch_ts,
            len(batches)
        )

        self._log(f"重建累积结果：群 {group_id}，{len(batches)} 个批次，{total_messages} 条消息")
        return True

    async def _merge_with_cumulative(self, group_id: str) -> bool:
        cumulative = await self._get_cumulative(group_id)
        if not cumulative:
            return await self._build_cumulative_from_batches(group_id)

        last_ts = cumulative.get('last_batch_timestamp', 0)
        cutoff = int(time.time()) - self.window_hours * 3600
        all_batches = await self.db.get_incremental_batches(group_id, cutoff)

        # 累积结果必须只对应“当前窗口内的批次”。如果窗口滑动、批次被清理或
        # window_hours 改过，旧 topics/quotes/total 会残留，必须先按窗口重建。
        window_total = sum(int(b.get('message_count', 0) or 0) for b in all_batches)
        if (not all_batches
                or int(cumulative.get('total_messages', 0) or 0) != window_total
                or int(cumulative.get('merged_batch_count', 0) or 0) != len(all_batches)):
            self._log(
                f"累积结果与当前窗口不一致（累计 {cumulative.get('total_messages', 0)} 条/"
                f"{cumulative.get('merged_batch_count', 0)} 批，窗口 {window_total} 条/"
                f"{len(all_batches)} 批），按窗口重建"
            )
            return await self._build_cumulative_from_batches(group_id)

        new_batches = [b for b in all_batches if b['start_timestamp'] > last_ts]

        if not new_batches:
            self._log(f"累积合并：群 {group_id} 无新批次")
            return True

        topic_counts = json.loads(cumulative.get('topic_counts_json', '{}'))
        quote_counts = json.loads(cumulative.get('quote_counts_json', '{}'))
        user_counts = json.loads(cumulative.get('user_counts_json', '{}'))
        topics = json.loads(cumulative.get('topics_json', '[]'))
        quotes = json.loads(cumulative.get('quotes_json', '[]'))
        active_users = json.loads(cumulative.get('active_users_json', '[]'))
        total_messages = cumulative.get('total_messages', 0)
        participants = cumulative.get('participants', 0)
        merged_batch_count = cumulative.get('merged_batch_count', 0)

        topic_meta = {t.get('title'): t for t in topics if t.get('title')}
        quote_meta = {q.get('text'): q for q in quotes if q.get('text')}
        user_meta = {u.get('name'): u for u in active_users if u.get('name')}

        last_batch_ts = cumulative.get('last_batch_timestamp', 0)

        for batch in new_batches:
            try:
                batch_topics = json.loads(batch['topics_json'])
                batch_quotes = json.loads(batch['quotes_json'])

                total_messages += batch.get('message_count', 0)
                last_batch_ts = max(last_batch_ts, batch.get('end_timestamp', 0))
                merged_batch_count += 1

                for topic in batch_topics:
                    title = topic.get('title', '')
                    if title:
                        topic_counts[title] = topic_counts.get(title, 0) + 1
                        if title not in topic_meta:
                            topic_meta[title] = topic

                for quote in batch_quotes:
                    text = quote.get('text', '')
                    if text:
                        quote_counts[text] = quote_counts.get(text, 0) + 1
                        if text not in quote_meta:
                            quote_meta[text] = quote

                users = json.loads(batch['active_users_json'])
                for user in users:
                    name = user.get('name', '')
                    if name:
                        user_counts[name] = user_counts.get(name, 0) + user.get('count', 0)
                        if name not in user_meta:
                            user_meta[name] = user

            except Exception as e:
                self._log(f"合并批次失败: {e}")

        # 构建话题候选池（按频次排序取前 topic_max * 2 条）
        topic_candidates = sorted(
            topic_meta.values(),
            key=lambda t: topic_counts.get(t.get('title', ''), 0),
            reverse=True
        )[:self.topic_max * 2]

        # 构建金句候选池（按频次排序取前 quote_max * 2 条）
        quote_candidates = sorted(
            quote_meta.values(),
            key=lambda q: quote_counts.get(q.get('text', ''), 0),
            reverse=True
        )[:self.quote_max * 2]

        # 活跃用户
        sorted_users = sorted(
            [{"name": k, "count": v} for k, v in user_counts.items()],
            key=lambda x: x.get('count', 0),
            reverse=True
        )[:self.active_users_max]

        # 收集历史锐评
        existing_sharp_comments = []
        sharp_comment = cumulative.get('sharp_comment', '')
        if sharp_comment:
            existing_sharp_comments.append(sharp_comment)

        # 统一调用 LLM 完成所有润色任务
        result = await self._generate_full_daily_with_llm(
            topic_candidates=topic_candidates,
            quote_candidates=quote_candidates,
            active_users=sorted_users,
            existing_sharp_comments=existing_sharp_comments
        )

        if result:
            topics = result.get('topics', [])[:self.topic_max]
            quotes = result.get('quotes', [])[:self.quote_max]
            if result.get('sharp_comment'):
                sharp_comment = result.get('sharp_comment')
        else:
            topics = topic_candidates[:self.topic_max]
            quotes = quote_candidates[:self.quote_max]

        await self._save_cumulative(
            group_id,
            topics,
            quotes,
            sorted_users,
            topic_counts,
            user_counts,
            total_messages,
            len(user_counts),
            last_batch_ts,
            merged_batch_count
        )

        self._log(f"累积合并：群 {group_id}，合并 {len(new_batches)} 个新批次，总计 {merged_batch_count} 个批次")
        return True

    # ============================================================
    # 辅助方法
    # ============================================================

    async def _get_nickname_by_user_id(self, group_id: str, user_id: str) -> str:
        try:
            cursor = await self.db._conn.execute(
                "SELECT nickname FROM messages WHERE session_id = ? AND user_id = ? ORDER BY timestamp DESC LIMIT 1",
                (group_id, user_id)
            )
            row = await cursor.fetchone()
            if row:
                return row[0]
        except Exception:
            pass
        return user_id

    # ============================================================
    # 清理过期数据
    # ============================================================

    async def _cleanup_old_messages(self):
        """清理过期消息和批次，并联动清理累积结果"""
        now = int(time.time())
        try:
            # 1. 清理过期消息
            if self.message_retention_days > 0:
                before = now - self.message_retention_days * 86400
                deleted = await self.db.delete_old_messages(before)
                if deleted:
                    self._log(f"清理了 {deleted} 条过期消息（保留 {self.message_retention_days} 天）")
            else:
                self._log("消息保留天数设为 0，永久保留")

            # 2. 清理过期增量批次
            if self.batch_retention_days > 0:
                before = now - self.batch_retention_days * 86400
                deleted_batches = await self.db.delete_old_batches(before)
                if deleted_batches:
                    self._log(f"清理了 {deleted_batches} 个过期增量批次（保留 {self.batch_retention_days} 天）")

                    groups = await self.db.get_all_groups()
                    deleted_cumulative_count = 0
                    for group_id in groups:
                        if await self._get_cumulative(group_id):
                            await self._delete_cumulative(group_id)
                            deleted_cumulative_count += 1
                    if deleted_cumulative_count:
                        self._log(f"删除了 {deleted_cumulative_count} 个累积结果（因批次清理，将在下次日报时自动重建）")
            else:
                self._log("增量批次保留天数设为 0，永久保留")

        except Exception as e:
            logger.error(f"[KiraDaily] 清理过期数据失败: {e}")

    async def _cleanup_old_reports(self):
        """清理旧报告：每次删除最旧的 N 个过期报告（N = report_cleanup_count）"""
        try:
            files = list(self.reports_dir.glob("*.png")) + list(self.reports_dir.glob("*.html")) + list(self.reports_dir.glob("*.txt"))
            if not files:
                self._log("无报告需要清理")
                return

            files.sort(key=lambda f: f.stat().st_mtime)

            if self.report_retention_days <= 0:
                self._log(f"报告保留天数设为 0，永久保留")
                return

            cutoff_time = time.time() - self.report_retention_days * 86400
            expired_files = [f for f in files if f.stat().st_mtime < cutoff_time]

            if not expired_files:
                self._log(f"无需清理：所有报告都在 {self.report_retention_days} 天内")
                return

            delete_count = min(len(expired_files), self.report_cleanup_count)
            to_delete = expired_files[:delete_count]

            if to_delete:
                for f in to_delete:
                    try:
                        f.unlink(missing_ok=True)
                        meta_path = f.with_suffix(".meta.json")
                        meta_path.unlink(missing_ok=True)
                    except Exception as e:
                        self._log(f"删除旧报告失败 {f}: {e}")
                self._log(f"清理了 {len(to_delete)} 个旧报告（共 {len(expired_files)} 个过期，本次删除 {len(to_delete)} 个）")
            else:
                self._log(f"无需清理：没有过期报告")

        except Exception as e:
            logger.error(f"[KiraDaily] 清理报告失败: {e}")

    # ============================================================
    # 安全执行分析
    # ============================================================

    async def _safe_do_analysis(self, group_id: str, user_id: str, immediate: bool = False):
        task_key = f"incremental_{group_id}" if immediate else f"full_{group_id}"
        if not immediate:
            try:
                await self._do_analysis(group_id, user_id)
            except Exception as e:
                logger.error(f"[KiraDaily] 后台分析任务失败 {group_id}: {e}")
            finally:
                self._running_analysis_tasks.discard(task_key)
            return
        if group_id in self._running_groups:
            self._log(f"即时增量跳过：群 {group_id} 已有分析任务在跑")
            self._running_analysis_tasks.discard(task_key)
            return
        self._running_groups.add(group_id)
        try:
            async with self._semaphore:
                await self._do_incremental_analysis(group_id)
        except Exception as e:
            logger.error(f"[KiraDaily] 后台分析任务失败 {group_id}: {e}")
        finally:
            self._running_groups.discard(group_id)
            self._running_analysis_tasks.discard(task_key)

    # ============================================================
    # 核心分析逻辑
    # ============================================================

    async def _do_analysis(self, group_id: str, user_id: str = "system", days: int = 0) -> bool:
        if group_id in self._running_groups:
            self._log(f"群 {group_id} 已有分析任务在跑，跳过本次触发")
            return True
        self._running_groups.add(group_id)
        try:
            return await self._do_analysis_locked(group_id, user_id, days)
        finally:
            self._running_groups.discard(group_id)

    async def _do_analysis_locked(self, group_id: str, user_id: str = "system", days: int = 0) -> bool:
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return True

        async with self._semaphore:
            try:
                if self._is_incremental_group(group_id):
                    self._log(f"增量模式：群 {group_id} 开始增量分析")
                    incremental_success = await self._do_incremental_analysis(group_id)

                    # 只有真正的错误（LLM 调用失败/异常）才回退到全量
                    if not incremental_success:
                        self._log(f"增量模式：群 {group_id} 增量分析出现错误，回退到全量分析")
                        return await self._do_full_analysis(group_id, user_id, days)

                    self._log(f"增量模式：群 {group_id} 使用累积结果生成日报")
                    bot_info = await self._get_bot_info(*self._adapter_for_group(group_id))
                    bot_self_id = bot_info.get("self_id", "")

                    # 1. 检查跨天
                    if await self._is_cross_day(group_id):
                        self._log(f"增量模式：群 {group_id} 跨天，重建累积结果")
                        await self._build_cumulative_from_batches(group_id)

                    # 2. 获取窗口内原始消息（用于统计数字）
                    window_start_ts = int(time.time()) - self.window_hours * 3600
                    all_window_msgs = await self.db.get_messages(group_id, window_start_ts, limit=None)

                    total_messages = len(all_window_msgs)
                    participants_count = len(set(m['user_id'] for m in all_window_msgs))
                    peak_hour = self._calculate_peak_hour(all_window_msgs)
                    self._log(f"实时统计：总消息数 {total_messages}，参与人数 {participants_count}，活跃时段 {peak_hour}")

                    # 3. 获取累积结果
                    cumulative = await self._get_cumulative(group_id)
                    if not cumulative:
                        self._log(f"增量模式：群 {group_id} 无累积结果，回退到全量分析")
                        return await self._do_full_analysis(group_id, user_id, days)

                    # 4. 从累积结果提取话题、金句
                    topics = json.loads(cumulative.get('topics_json', '[]'))
                    quotes = json.loads(cumulative.get('quotes_json', '[]'))

                    # 5. 活跃用户直接从原始消息统计
                    user_msg_counts: Dict[str, int] = defaultdict(int)
                    user_nickname_map: Dict[str, str] = {}

                    for msg in all_window_msgs:
                        uid = msg.get('user_id', '')
                        if not uid or uid == bot_self_id:
                            continue
                        user_msg_counts[uid] += 1
                        if msg.get('nickname'):
                            user_nickname_map[uid] = msg['nickname']

                    sorted_user_items = sorted(
                        user_msg_counts.items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:self.active_users_max]

                    active_users_from_db = []
                    for uid, count in sorted_user_items:
                        nickname = user_nickname_map.get(uid, uid)
                        active_users_from_db.append({
                            'name': f"{nickname}#{uid}",
                            'count': count
                        })

                    self._log(f"活跃用户统计：从 {total_messages} 条消息中统计出 {len(active_users_from_db)} 位活跃用户")

                    # 6. 回退检查
                    if not topics and not quotes and not active_users_from_db:
                        self._log(f"增量模式：合并后仍无数据，回退到全量分析")
                        return await self._do_full_analysis(group_id, user_id, days)

                    # 7. 组装最终结果
                    sharp_comment = cumulative.get('sharp_comment', '')
                    header_comment = cumulative.get('header_comment', '')

                    # 如果累积结果中没有 header_comment 和 sharp_comment，需要重新生成
                    if not header_comment or not sharp_comment:
                        result = await self._generate_full_daily_with_llm(
                            topic_candidates=topics,
                            quote_candidates=quotes,
                            active_users=active_users_from_db,
                            existing_sharp_comments=[sharp_comment] if sharp_comment else []
                        )
                        if result:
                            topics = result.get('topics', topics)
                            quotes = result.get('quotes', quotes)
                            best_quote = result.get('best_quote')
                            header_comment = result.get('header_comment', header_comment)
                            sharp_comment = result.get('sharp_comment', sharp_comment)
                        else:
                            best_quote = quotes[0] if quotes else None
                    else:
                        best_quote = quotes[0] if quotes else None

                    final_result = {
                        'stats': {
                            'total_messages': total_messages,
                            'participants': participants_count,
                            'peak_hour': peak_hour
                        },
                        'topics': topics,
                        'active_users': active_users_from_db,
                        'quotes': quotes,
                        'best_quote': best_quote,
                        'sharp_comment': sharp_comment,
                        'header_comment': header_comment
                    }

                    self._log(f"增量模式：群 {group_id} 日报生成完成，总消息数 {total_messages}，参与人数 {participants_count}")

                    # 生成报告
                    if all_window_msgs:
                        actual_start_ts = all_window_msgs[0]['timestamp']
                        actual_end_ts = all_window_msgs[-1]['timestamp']
                    else:
                        actual_start_ts = window_start_ts
                        actual_end_ts = int(time.time())

                    report_path = await self._generate_report(
                        group_id, final_result, [],
                        False,
                        actual_start_ts, actual_end_ts,
                        is_incremental=True,
                        bot_info=bot_info
                    )
                    await self._send_report(group_id, report_path)
                    self._update_cooldown(group_id)
                    return True

                return await self._do_full_analysis(group_id, user_id, days)

            except Exception as e:
                logger.error(f"[KiraDaily] 分析群 {group_id} 失败: {e}")
                await self._send_text_to_group(group_id, self.msg_failed.format(error=str(e)[:100]))
                return True

    # ============================================================
    # 全量分析
    # ============================================================

    async def _do_full_analysis(self, group_id: str, user_id: str = "system", days: int = 0) -> bool:
        # days>0 时用调用方指定的天数（后台并发任务隔离用），否则用实例配置
        if days <= 0:
            days = self.analysis_days
        messages = await self.db.get_messages(group_id, int(time.time()) - days * 86400)
        total_msgs = len(messages)
        if total_msgs < self.min_messages_threshold:
            await self._send_text_to_group(
                group_id,
                self.msg_too_few.format(count=total_msgs, threshold=self.min_messages_threshold)
            )
            return True

        # 统计口径用“截断前的全量消息”：总消息数、参与人数、最活跃时段、统计范围保持一致。
        # 统计完立刻释放全量列表，后续 LLM 调用期间只保留真正要送模的 messages。
        all_messages = messages
        start_time = all_messages[0]['timestamp'] if all_messages else 0
        end_time = all_messages[-1]['timestamp'] if all_messages else 0
        participants_count = len(set(m['user_id'] for m in all_messages))
        peak_hour = self._calculate_peak_hour(all_messages)
        del all_messages

        truncated = False
        if total_msgs > self.max_messages_per_analysis:
            messages = messages[-self.max_messages_per_analysis:]
            truncated = True

        bot_info = await self._get_bot_info(*self._adapter_for_group(group_id))
        bot_self_id = bot_info.get("self_id", "")

        prompt = await self._build_analysis_prompt(messages, truncated, bot_self_id=bot_self_id)
        result = await self._call_llm(prompt)
        if not result:
            await self._send_text_to_group(group_id, "❌ LLM 分析失败，请重试")
            return True

        quotes = result.get('quotes', [])
        filtered_quotes = []
        for q in quotes:
            sender = q.get('sender', '')
            _, uid = parse_user_tag(sender)
            if uid and uid == bot_self_id:
                continue
            filtered_quotes.append(q)
        result['quotes'] = filtered_quotes

        if result.get('best_quote'):
            best = result['best_quote']
            sender = best.get('sender', '')
            _, uid = parse_user_tag(sender)
            if uid and uid == bot_self_id:
                result['best_quote'] = None

        stats = result.get('stats', {})
        stats['total_messages'] = total_msgs
        stats['participants'] = participants_count
        stats['peak_hour'] = peak_hour
        result['stats'] = stats

        result.setdefault('header_comment', '今日群聊，热闹非凡！')
        result.setdefault('sharp_comment', '')
        if not result.get('topics'):
            result['topics'] = []
        while len(result['topics']) < self.topic_min:
            result['topics'].append({'title': '？', 'detail': '暂无足够话题', 'roast': ''})
        if self.quote_max > 0 and not result.get('quotes'):
            result['quotes'] = []
        result['best_quote'] = result.get('best_quote')

        report_path = await self._generate_report(
            group_id, result, messages, truncated, start_time, end_time,
            is_incremental=False,
            bot_info=bot_info
        )
        await self._send_report(group_id, report_path)
        self._update_cooldown(group_id)
        return True

    # ============================================================
    # 生成报告
    # ============================================================

    async def _generate_report(
        self,
        group_id: str,
        analysis_result: dict,
        messages: List[dict],
        truncated: bool,
        start_time: int,
        end_time: int,
        is_incremental: bool = False,
        bot_info: Optional[Dict[str, str]] = None,
    ) -> Path:
        if bot_info is None:
            bot_info = await self._get_bot_info(*self._adapter_for_group(group_id))
        bot_self_id = bot_info.get("self_id", "0")
        bot_nickname = bot_info.get("nickname") or self._bot_nickname or "AI"
        bot_avatar = bot_info.get("avatar") or self._bot_avatar or ""

        group_name = await self._get_group_name(group_id)
        group_avatar = await self.avatar_cache.get_group_avatar(group_id)

        border_colors = ["#D4A574", "#C9B0A0", "#E8C8A0", "#D4B8A8", "#C4A88C", "#E0C8B8", "#F5D0B0", "#D9B8A0"]
        group_avatar_border_color = random.choice(border_colors)
        title_colors = ["#6B4A3A", "#A67B5B", "#8B6B4A", "#C49A6C", "#B8896A", "#9C7A5A", "#7A5A4A"]
        title_color = random.choice(title_colors)
        title_gradient = f"linear-gradient(135deg, {random.choice(title_colors)}, {random.choice(title_colors)})"
        title_fonts = [
            "'Comic Sans MS', cursive", "'KaiTi', '楷体', serif",
            "'STZhongsong', '华文中宋', serif", "'Yuanti SC', '圆体', sans-serif",
            "'ZCOOL KuaiLe', cursive", "'Ma Shan Zheng', cursive"
        ]
        title_font = random.choice(title_fonts)

        user_avatars = {}
        user_styles = {}
        border_colors_list = [
            "#D4A574", "#C9B0A0", "#E8C8A0", "#D4B8A8", "#C4A88C", "#E0C8B8",
            "#F5D0B0", "#D9B8A0", "#CFAFA0", "#E6C8B0", "#DDBFA8", "#F0D0C0"
        ]
        shuffled_colors = WARM_COLORS.copy()
        random.shuffle(shuffled_colors)

        active_users = analysis_result.get('active_users', [])
        for idx, user in enumerate(active_users):
            name_tag = user.get('name', '')
            display_name, uid = parse_user_tag(name_tag)
            if uid and uid.isdigit():
                if uid == bot_self_id:
                    continue
                avatar = await self.avatar_cache.get_user_avatar(uid)
                user_avatars[name_tag] = avatar
                border_color = random.choice(border_colors_list)
                color = shuffled_colors[idx % len(shuffled_colors)]
                user_styles[name_tag] = {
                    "border_color": border_color,
                    "name_color": color,
                    "display_name": display_name or name_tag
                }
            else:
                avatar = await self.avatar_cache.get_user_avatar(name_tag) if name_tag.isdigit() else bot_avatar
                user_avatars[name_tag] = avatar
                border_color = random.choice(border_colors_list)
                color = shuffled_colors[idx % len(shuffled_colors)]
                user_styles[name_tag] = {
                    "border_color": border_color,
                    "name_color": color,
                    "display_name": name_tag
                }

        filtered_active_users = []
        for user in active_users:
            name_tag = user.get('name', '')
            _, uid = parse_user_tag(name_tag)
            if uid and uid == bot_self_id:
                continue
            filtered_active_users.append(user)
        analysis_result['active_users'] = filtered_active_users

        for quote in analysis_result.get('quotes', []):
            sender_tag = quote.get('sender', '')
            _, uid = parse_user_tag(sender_tag)
            if uid and uid.isdigit() and sender_tag not in user_avatars:
                if uid == bot_self_id:
                    continue
                avatar = await self.avatar_cache.get_user_avatar(uid)
                user_avatars[sender_tag] = avatar
                if sender_tag not in user_styles:
                    display_name, _ = parse_user_tag(sender_tag)
                    user_styles[sender_tag] = {
                        "border_color": random.choice(border_colors_list),
                        "name_color": random.choice(WARM_COLORS),
                        "display_name": display_name or sender_tag
                    }
        if analysis_result.get('best_quote'):
            sender_tag = analysis_result['best_quote'].get('sender', '')
            _, uid = parse_user_tag(sender_tag)
            if uid and uid.isdigit() and sender_tag not in user_avatars:
                if uid == bot_self_id:
                    analysis_result['best_quote'] = None
                else:
                    avatar = await self.avatar_cache.get_user_avatar(uid)
                    user_avatars[sender_tag] = avatar
                    if sender_tag not in user_styles:
                        display_name, _ = parse_user_tag(sender_tag)
                        user_styles[sender_tag] = {
                            "border_color": random.choice(border_colors_list),
                            "name_color": random.choice(WARM_COLORS),
                            "display_name": display_name or sender_tag
                        }

        stats = analysis_result.get('stats', {})

        if is_incremental:
            stats['max_messages'] = "∞"
            stats['display_limit'] = "∞"
        else:
            limit = self.max_messages_per_analysis
            stats['max_messages'] = limit
            stats['display_limit'] = limit
            if truncated:
                stats['is_truncated'] = True
            else:
                stats['is_truncated'] = False

        stats['start_time'] = self._dt(start_time).strftime('%m-%d %H:%M') if start_time > 0 else "未知"
        stats['end_time'] = self._dt(end_time).strftime('%m-%d %H:%M') if end_time > 0 else "未知"
        analysis_result['stats'] = stats

        # 报告日期跟随“分析窗口结束时间”，而不是任务实际完成时间：
        # 定时任务跨过午夜完成、或手动补生成时，日期不会跳成第二天。
        report_time = self._dt(end_time) if end_time > 0 else self._now()

        html = self.renderer.render_template(
            theme=self.theme,
            group_id=group_id,
            group_name=group_name,
            group_avatar=group_avatar,
            group_avatar_border_color=group_avatar_border_color,
            title_color=title_color,
            title_gradient=title_gradient,
            title_font=title_font,
            analysis=analysis_result,
            user_avatars=user_avatars,
            user_styles=user_styles,
            bot_avatar=bot_avatar or '',
            bot_nickname=bot_nickname or 'AI',
            generate_time=self._now(),
            report_time=report_time,
            topic_min=self.topic_min,
            topic_max=self.topic_max,
            quote_max=self.quote_max,
            active_users_max=self.active_users_max
        )

        html_path = self.reports_dir / f"{group_id.replace(':', '_')}_{int(time.time())}.html"
        html_path.write_text(html, encoding='utf-8')

        if self.output_format == 'image':
            png_path = html_path.with_suffix('.png')
            await self.renderer.render_to_image(html, str(png_path))
            return png_path
        else:
            txt_path = html_path.with_suffix('.txt')
            txt_content = self._generate_text_report(analysis_result)
            txt_path.write_text(txt_content, encoding='utf-8')
            return txt_path

    # ============================================================
    # 文本报告生成
    # ============================================================

    def _generate_text_report(self, analysis_result: dict) -> str:
        stats = analysis_result.get('stats', {})
        topics = analysis_result.get('topics', [])
        active_users = analysis_result.get('active_users', [])
        quotes = analysis_result.get('quotes', [])
        sharp_comment = analysis_result.get('sharp_comment', '')
        header_comment = analysis_result.get('header_comment', '')

        lines = []
        lines.append(f"📊 {header_comment}")
        total = stats.get('total_messages', 0)
        max_msgs = stats.get('max_messages')
        if max_msgs:
            if str(max_msgs) == "∞":
                lines.append(f"总消息数: {total} (全部)")
            else:
                lines.append(f"总消息数: {total}/{max_msgs} (仅分析最新 {max_msgs} 条)")
        else:
            lines.append(f"总消息数: {total}")
        lines.append(f"参与人数: {stats.get('participants', 0)}")
        lines.append(f"最活跃时段: {stats.get('peak_hour', '未知')}")
        if stats.get('start_time') and stats.get('end_time'):
            lines.append(f"统计范围: {stats['start_time']} ~ {stats['end_time']}")
        lines.append("")
        lines.append("💬 热门话题:")
        for topic in topics[:self.topic_max]:
            lines.append(f"  • {topic.get('title', '')}: {topic.get('detail', '')}")
            if topic.get('roast'):
                lines.append(f"    🤖 吐槽: {topic.get('roast')}")
        lines.append("")
        lines.append("👤 活跃用户:")
        for user in active_users[:self.active_users_max]:
            name_tag = user.get('name', '')
            display_name, _ = parse_user_tag(name_tag)
            lines.append(f"  • {display_name or name_tag}: {user.get('count', 0)}条")
        lines.append("")
        if quotes:
            lines.append("💡 金句:")
            for q in quotes[:self.quote_max]:
                sender = q.get('sender', '')
                display_name, _ = parse_user_tag(sender)
                lines.append(f"  • {display_name or sender}: {q.get('text', '')}")
        best = analysis_result.get('best_quote')
        if best:
            sender = best.get('sender', '')
            display_name, _ = parse_user_tag(sender)
            lines.append(f"  ⭐ 最佳: {display_name or sender}: {best.get('text', '')}")
            lines.append(f"     理由: {best.get('reason', '')}")
        lines.append("")
        if sharp_comment:
            lines.append(f"🤖 锐评: {sharp_comment}")
        return "\n".join(lines)

    # ============================================================
    # 发送报告
    # ============================================================

    async def _send_report(self, group_id: str, report_path: Path):
        adapter, _adapter_name = self._adapter_for_group(group_id)
        if not adapter:
            logger.error(
                "[KiraDaily] 未找到可用的 QQ 适配器，无法发送日报"
                "（可在插件配置里填写适配器实例名，或留空自动探测）"
            )
            return
        parts = group_id.split(":")
        if len(parts) >= 3:
            qq_group_id = parts[2]
        else:
            qq_group_id = group_id
        if self.output_format == "image":
            await self._send_group_msg(adapter, qq_group_id, MessageChain([Image(str(report_path))]))
        else:
            text_content = report_path.read_text(encoding="utf-8")
            for chunk in [text_content[i:i+500] for i in range(0, len(text_content), 500)]:
                await self._send_group_msg(adapter, qq_group_id, MessageChain([Text(chunk)]))
                await asyncio.sleep(0.5)

    # ============================================================
    # 定时任务（每天执行清理 + 日报）
    # ============================================================

    async def _scheduler_loop(self):
        while self._running:
            try:
                now = self._now()
                target_time = datetime.strptime(self.auto_analysis_time, "%H:%M")
                target_datetime = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                if target_datetime <= now:
                    target_datetime += timedelta(days=1)
                wait_seconds = (target_datetime - now).total_seconds()
                self._log(f"距离下次自动日报还有 {wait_seconds:.0f} 秒")
                await asyncio.sleep(wait_seconds)
                if not self._running:
                    break

                self._log("执行每日清理...")
                await self._cleanup_old_messages()
                await self._cleanup_old_reports()

                await self._run_auto_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[KiraDaily] 定时任务异常: {e}")
                await asyncio.sleep(60)

    async def _run_auto_analysis(self):
        self._log("开始执行自动日报分析")

        if self.auto_enabled_groups:
            groups = self.auto_enabled_groups
        else:
            if self.enabled_groups:
                groups = self.enabled_groups
            else:
                groups = await self.db.get_all_groups()
        if not groups:
            self._log("没有需要分析的群")
            return
        tasks = []
        for group_id in groups:
            tasks.append(self._do_analysis(group_id, "system"))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        self._log(f"自动日报完成: {success_count}/{len(groups)} 群成功")

    # ============================================================
    # 增量定时任务
    # ============================================================

    async def _incremental_scheduler_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.incremental_interval_minutes * 60)
                if not self._running:
                    break
                await self._run_incremental_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[KiraDaily] 增量调度异常: {e}")
                await asyncio.sleep(60)

    async def _run_incremental_analysis(self):
        self._log("开始执行增量分析")
        if self.incremental_group_list:
            groups = self.incremental_group_list
        else:
            if self.enabled_groups:
                groups = self.enabled_groups
            else:
                groups = await self.db.get_all_groups()
        if not groups:
            return
        async def _run_one(gid: str):
            if gid in self._running_groups:
                self._log(f"增量分析跳过：群 {gid} 已有任务在跑")
                return True
            self._running_groups.add(gid)
            try:
                async with self._semaphore:
                    return await self._do_incremental_analysis(gid)
            finally:
                self._running_groups.discard(gid)

        results = await asyncio.gather(*[_run_one(g) for g in groups], return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        self._log(f"增量分析完成: {success_count}/{len(groups)} 群成功")

    # ============================================================
    # 命令处理（手动触发）
    # ============================================================

    @on.im_message(priority=Priority.MEDIUM)
    async def handle_command(self, event: KiraMessageEvent):
        if not self.enable_command_trigger or not event.is_group_message():
            return
        text = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text)).strip()
        if text not in self.command_prefixes:
            return

        # 命令消息不进入 chat 的 buffer：在第一个 await 之前就 stop。
        # 否则 chat 的防抖会先把 "/日报" 刷进 batch，bot 可能先回一句“好的”。
        event.stop()

        group_id = self._get_group_id_from_event(event)
        if not group_id:
            await self.ctx.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_group)])
            )
            return
        if not self._is_group_enabled(group_id):
            await self.ctx.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_enabled)])
            )
            return
        user_id = self._get_user_id_from_event(event)
        if group_id in self._running_groups:
            await self.ctx.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_processing)])
            )
            return
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return

        await self.ctx.send_message_chain(
            session=self._get_sid(event),
            chain=MessageChain([Text(self.msg_processing)])
        )

        sid = self._get_sid(event)
        if sid in self._gen_tasks and not self._gen_tasks[sid].done():
            return

        async def _run():
            try:
                await self._do_analysis(group_id, user_id)
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("[KiraDaily] 后台命令生成日报失败")

        task = asyncio.create_task(_run())
        self._gen_tasks[sid] = task

        def _cleanup(t, _sid=sid):
            if self._gen_tasks.get(_sid) is t:
                self._gen_tasks.pop(_sid, None)

        task.add_done_callback(_cleanup)

    # ============================================================
    # LLM 工具（自然语言触发）
    # ============================================================

    @register_tool(
        name="generate_daily_report",
        description="为当前群生成一份群聊日报，包含消息统计、热门话题、活跃用户、金句和AI锐评。"
                   "当用户要求总结群聊、生成日报、看看今天的群聊情况时调用此工具。"
                   "注意：每次调用会消耗较多Token，请确认用户确实需要日报再调用。",
        params={
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "default": 1,
                    "description": "分析最近几天的消息，默认1天"
                }
            },
            "required": []
        }
    )
    async def tool_generate_report(self, event: KiraMessageBatchEvent, days: int = 1) -> str:
        if not self.enable_natural_language_trigger:
            await self._send_text_to_group(
                self._get_group_id_from_event(event) or "unknown",
                self.msg_natural_disabled
            )
            return "✅ 已向用户说明自然语言触发已禁用。"
        group_id = self._get_group_id_from_event(event)
        if not group_id:
            return "✅ 已向用户说明：请在群聊中使用此功能。"
        if not self._is_group_enabled(group_id):
            await self._send_text_to_group(group_id, self.msg_not_enabled)
            return "✅ 已向用户说明该群未启用日报功能。"
        # days 规范化后仅作为本任务局部值使用；不改实例 analysis_days，
        # 避免多群并发任务互相污染（全量分支显式传参隔离）
        days = min(max(days or 1, 1), 7)
        user_id = self._get_user_id_from_event(event)
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return "✅ 冷却中，已向用户说明。"
        sid = self._get_sid(event)

        # 同一会话/同一群已有生成任务在跑 → 不重复触发
        if sid in self._gen_tasks and not self._gen_tasks[sid].done():
            return "⏳ 该群日报正在生成中，完成后会自动发送，请告知用户稍候，不要重复请求。"
        if group_id in self._running_groups:
            return "⏳ 该群日报正在生成中，完成后会自动发送，请告知用户稍候，不要重复请求。"

        async def _run():
            try:
                group_key = group_id.replace(":", "_")
                before = self._report_files()
                # 统一入口：并发去重、信号量、冷却、增量/全量分支都在 _do_analysis 内
                await self._do_analysis(group_id, user_id, days)
                # 报告文件名含群ID+时间戳，每次生成必为新文件：按本群前缀判断是否真正生成
                new_files = [f for f in (self._report_files() - before)
                             if f.startswith(f"{group_key}_")]
                if new_files:
                    await self._publish_notice(
                        sid,
                        "系统通知：日报已生成并发送到群里，请用一两句话告知用户日报已生成，不要重复发送日报。"
                    )
                else:
                    await self._publish_notice(
                        sid,
                        "系统通知：日报未生成（可能是消息不足、冷却中或生成失败，插件已发送相应提示），"
                        "请向用户简要说明情况，不要重复发送日报。"
                    )
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("[KiraDaily] 后台生成日报失败")
                await self._publish_notice(sid, "系统通知：日报生成失败，请告知用户稍后重试。")
            finally:
                self._gen_tasks.pop(sid, None)

        task = asyncio.create_task(_run())
        self._gen_tasks[sid] = task
        return ("✅ 已开始生成日报（LLM 分析与渲染需要一点时间），"
                "完成后会自动发送到群里，请告知用户稍候。")
