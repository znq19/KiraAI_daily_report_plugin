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

        # ---- 分析参数 ----
        analysis = cfg.get("section_analysis", {})
        self.analysis_days = analysis.get("analysis_days", 1)
        self.max_messages_per_analysis = analysis.get("max_messages_per_analysis", 500)
        self.min_messages_threshold = analysis.get("min_messages_threshold", 10)
        self.auto_analysis_time = analysis.get("auto_analysis_time", "23:59")
        self.enable_auto_analysis = analysis.get("enable_auto_analysis", True)
        self.peak_window_minutes = analysis.get("peak_window_minutes", 90)

        # ---- 内容设置 ----
        content = cfg.get("section_content", {})
        self.enable_sharp_comment = content.get("enable_sharp_comment", True)
        self.inject_persona = content.get("inject_persona", True)
        self.persona_id = content.get("persona_id", "default")
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
        self.max_incremental_batches = incremental.get("max_incremental_batches", 5)

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
        self._collected_bot_messages: Set[str] = set()
        self._running_analysis_tasks: Set[str] = set()

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
                self.ctx.llm_api.unregister_tool("generate_daily_report")
                self._log("自然语言触发已禁用，已从LLM工具列表中移除 generate_daily_report")
            except Exception as e:
                self._log(f"注销工具失败（可能未注册）: {e}")
        else:
            self._log("自然语言触发已启用")

        await self._cleanup_old_reports()
        await self._cleanup_old_messages()

        if self.enable_auto_analysis:
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            logger.info(f"[KiraDaily] 定时任务已启动，每天 {self.auto_analysis_time} 执行")

        if self.enable_incremental_mode:
            self._incremental_scheduler_task = asyncio.create_task(self._incremental_scheduler_loop())
            logger.info(f"[KiraDaily] 增量分析已启动，间隔 {self.incremental_interval_minutes} 分钟")

        logger.info("[KiraDaily] 初始化完成")

    async def terminate(self):
        self._running = False
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        if self._incremental_scheduler_task and not self._incremental_scheduler_task.done():
            self._incremental_scheduler_task.cancel()
            try:
                await self._incremental_scheduler_task
            except asyncio.CancelledError:
                pass
        await self.db.close()
        logger.info("[KiraDaily] 已卸载")

    def _log(self, msg: str):
        if self.verbose_log:
            logger.info(f"[KiraDaily] {msg}")

    # ============================================================
    # 获取 Bot 信息
    # ============================================================

    async def _fetch_bot_info(self):
        try:
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
            self._bot_self_id = None
            self._bot_nickname = None
            self._bot_avatar = None

            if adapter:
                self._bot_self_id = str(adapter.info.config.get("self_id", ""))
                if not self._bot_self_id:
                    if hasattr(adapter, "bot") and hasattr(adapter.bot, "uin"):
                        self._bot_self_id = str(adapter.bot.uin)
                    elif hasattr(adapter, "bot") and hasattr(adapter.bot, "self_id"):
                        self._bot_self_id = str(adapter.bot.self_id)

                if self._bot_self_id:
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                    if self.bot_nickname_override:
                        self._bot_nickname = self.bot_nickname_override
                    else:
                        if hasattr(adapter, "get_login_info"):
                            try:
                                info = await adapter.get_login_info()
                                if info and info.get("nickname"):
                                    self._bot_nickname = info["nickname"]
                            except Exception:
                                pass
                        if not self._bot_nickname and hasattr(adapter, "bot") and hasattr(adapter.bot, "send_action"):
                            try:
                                result = await adapter.bot.send_action("get_login_info", {})
                                if result and result.get("status") == "ok":
                                    data = result.get("data", {})
                                    if data.get("nickname"):
                                        self._bot_nickname = data["nickname"]
                            except Exception:
                                pass
                        if not self._bot_nickname:
                            self._bot_nickname = "AI"
                else:
                    self._bot_self_id = self.ctx.config.get("bot_config.bot.self_id", "")
                    if self._bot_self_id:
                        self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                        self._bot_nickname = self.bot_nickname_override or "AI"
                    else:
                        self._bot_self_id = "0"
                        self._bot_nickname = self.bot_nickname_override or "AI"
                        self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")
            else:
                self._bot_self_id = self.ctx.config.get("bot_config.bot.self_id", "")
                if self._bot_self_id:
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                    self._bot_nickname = self.bot_nickname_override or "AI"
                else:
                    self._bot_self_id = "0"
                    self._bot_nickname = self.bot_nickname_override or "AI"
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")
        except Exception as e:
            self._log(f"获取 bot 信息失败: {e}")
            self._bot_self_id = "0"
            self._bot_nickname = self.bot_nickname_override or "AI"
            self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")

    # ============================================================
    # 获取群名称
    # ============================================================

    async def _get_group_name(self, group_id: str) -> str:
        try:
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
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
                sid = event.sid
                if sid.startswith("qq:gm:"):
                    return sid
                return None
            if hasattr(event, "message") and hasattr(event.message, "group"):
                if event.message.group:
                    return f"qq:gm:{event.message.group.group_id}"
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
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
            if not adapter:
                return
            parts = group_id.split(":")
            if len(parts) >= 3:
                qq_group_id = parts[2]
            else:
                qq_group_id = group_id
            await adapter.send_group_message(qq_group_id, MessageChain([Text(text)]))
        except Exception as e:
            self._log(f"发送文本到群 {group_id} 失败: {e}")

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

        sender_nickname = event.message.sender.nickname if event.message.sender else ""
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
                        asyncio.create_task(
                            self._safe_do_analysis(group_id, "system_immediate", immediate=True)
                        )

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
        if hasattr(last_msg, 'group') and last_msg.group:
            group_id = f"qq:gm:{last_msg.group.group_id}"
        else:
            return

        if not group_id or not self._is_group_enabled(group_id):
            return

        user_id = str(last_msg.sender.user_id) if last_msg.sender else None
        self_id = str(event.self_id) if hasattr(event, 'self_id') else None

        if not self_id or user_id != self_id:
            return

        msg_id = str(last_msg.message_id) if hasattr(last_msg, 'message_id') else None
        if msg_id and msg_id in self._collected_bot_messages:
            return
        if msg_id:
            self._collected_bot_messages.add(msg_id)

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
            nickname = self._bot_nickname or "Bot"
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
    # 活跃时段精确统计算法
    # ============================================================

    def _calculate_peak_hour(self, messages: List[dict]) -> str:
        """
        基于消息时间戳，计算最活跃的时间段。
        使用滑动窗口找到消息最密集的时间区间，然后收缩到实际消息边界。
        永不返回"未知"，总是返回一个合理的时间范围。
        """
        if not messages:
            return "暂无消息"

        # 提取所有时间戳并排序
        timestamps = sorted([msg['timestamp'] for msg in messages])
        n = len(timestamps)

        # 只有1条消息：显示该时间点
        if n == 1:
            dt = datetime.fromtimestamp(timestamps[0])
            return f"{dt.strftime('%H:%M')}"

        # 只有2条消息：显示这2条的时间范围
        if n == 2:
            start_dt = datetime.fromtimestamp(timestamps[0])
            end_dt = datetime.fromtimestamp(timestamps[1])
            if start_dt.date() == end_dt.date():
                return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
            else:
                return f"{start_dt.strftime('%m-%d %H:%M')}-{end_dt.strftime('%m-%d %H:%M')}"

        # ---- 3条消息以上：滑动窗口算法 ----
        window_seconds = self.peak_window_minutes * 60
        total_span = timestamps[-1] - timestamps[0]

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

        # ---- 判断是否属于"均匀分布"（无明显峰值） ----
        # 如果最佳窗口的消息数占比低于 35%，或者最佳窗口跨度占了总跨度的 80% 以上
        if (best_count / n < 0.35) or (best_count > 0 and (timestamps[best_end_idx] - timestamps[best_start_idx]) / max(total_span, 1) > 0.8):
            # 回退到全局范围，但限制最大显示 3 小时
            max_display_seconds = 3 * 3600  # 3 小时
            if total_span <= max_display_seconds:
                # 全局范围不大，直接显示全局
                start_ts = timestamps[0]
                end_ts = timestamps[-1]
            else:
                # 全局范围太大，取最近 3 小时的消息
                cutoff_ts = timestamps[-1] - max_display_seconds
                # 找到 cutoff_ts 之后的第一个消息索引
                for idx in range(n):
                    if timestamps[idx] >= cutoff_ts:
                        start_ts = timestamps[idx]
                        break
                else:
                    start_ts = timestamps[0]
                end_ts = timestamps[-1]
        else:
            # 有明显峰值，使用最佳窗口
            # 进一步收缩：在最佳窗口内，找到包含至少 80% 消息的最短子区间
            sub_window = window_seconds * 0.5
            sub_best_start = best_start_idx
            sub_best_end = best_end_idx
            sub_best_count = 0
            sub_j = best_start_idx
            for i in range(best_start_idx, best_end_idx + 1):
                if sub_j < i:
                    sub_j = i
                while sub_j + 1 <= best_end_idx and timestamps[sub_j + 1] - timestamps[i] <= sub_window:
                    sub_j += 1
                count = sub_j - i + 1
                if count > sub_best_count:
                    sub_best_count = count
                    sub_best_start = i
                    sub_best_end = sub_j
            # 如果收缩后的子区间保留了原窗口至少 70% 的消息，使用收缩结果
            if sub_best_count >= best_count * 0.7:
                start_ts = timestamps[sub_best_start]
                end_ts = timestamps[sub_best_end]
            else:
                start_ts = timestamps[best_start_idx]
                end_ts = timestamps[best_end_idx]

        # 格式化输出
        start_dt = datetime.fromtimestamp(start_ts)
        end_dt = datetime.fromtimestamp(end_ts)

        # 如果开始和结束在同一天，只显示时间
        if start_dt.date() == end_dt.date():
            return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
        else:
            # 跨天情况，显示日期+时间
            return f"{start_dt.strftime('%m-%d %H:%M')}-{end_dt.strftime('%m-%d %H:%M')}"

    # ============================================================
    # 统一润色方法
    # ============================================================

    async def _generate_final_commentary(
        self,
        topics: List[dict],
        quotes: List[dict],
        active_users: List[dict],
        existing_sharp_comments: List[str] = None
    ) -> tuple[str, str, Optional[dict]]:
        context_parts = []
        if topics:
            topic_summary = "、".join([t.get('title', '') for t in topics[:3]])
            context_parts.append(f"热门话题：{topic_summary}")
        if quotes:
            quote_list = [f'"{q.get("text", "")}"（{q.get("sender", "未知")}）' for q in quotes[:5]]
            context_parts.append(f"候选金句：{'; '.join(quote_list)}")
        if active_users:
            user_summary = "、".join([u.get('name', '') for u in active_users[:3]])
            context_parts.append(f"活跃用户：{user_summary}")

        context = "\n".join(context_parts) if context_parts else "今日群聊暂无突出内容"

        sharp_ref = ""
        if existing_sharp_comments:
            latest_sharp = existing_sharp_comments[-1]
            sharp_ref = f"\n历史锐评风格参考（仅作风格参考，不要照抄）：{latest_sharp}"

        prompt = f"""根据以下群聊汇总信息，完成三项任务：

1. 生成一句简短有趣的一句话总结（header），不超过20字，不带表情符号。
2. 生成一句毒舌/幽默的 AI 锐评（sharp），不超过30字，贴合群聊氛围。
3. 从「候选金句」中评选出最佳金句，并给出精炼的评选理由（不超过15字）。
{sharp_ref}

汇总信息：
{context}

请严格按以下 JSON 格式输出：
{{
  "header_comment": "...",
  "sharp_comment": "...",
  "best_quote": {{
    "text": "最佳金句原文",
    "sender": "发送者（必须保持完整格式，如 周武#123456）",
    "reason": "评选理由"
  }}
}}
"""

        try:
            result = await self._call_llm(prompt)
            if isinstance(result, dict):
                header = result.get("header_comment", "今日群聊，热闹非凡").strip()
                sharp = result.get("sharp_comment", "").strip()
                best = result.get("best_quote")
                if best and isinstance(best, dict) and best.get('text') and best.get('sender'):
                    return header, sharp, best
                if quotes:
                    return header, sharp, quotes[0]
                return header, sharp, None
        except Exception as e:
            self._log(f"生成最终评论失败: {e}")

        default_best = quotes[0] if quotes else None
        return "今日群聊，热闹非凡", "", default_best

    # ============================================================
    # 全量分析 Prompt
    # ============================================================

    async def _build_analysis_prompt(self, messages: List[dict], truncated: bool) -> str:
        if self._bot_self_id:
            messages = [m for m in messages if m['user_id'] != self._bot_self_id]

        msg_lines = []
        for msg in messages[-200:]:
            ts = datetime.fromtimestamp(msg["timestamp"]).strftime("%H:%M")
            user_tag = f"{msg['nickname']}#{msg['user_id']}"
            msg_lines.append(f"[{ts}] {user_tag}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        persona_prompt = ""
        if self.inject_persona:
            try:
                if self.persona_id and self.persona_id != "default":
                    persona = await self.ctx.persona_mgr.get_persona(self.persona_id)
                else:
                    personas = await self.ctx.persona_mgr.list_personas()
                    persona = personas[0] if personas else None
                if persona:
                    persona_prompt = f"\n请以以下人设风格进行分析和点评：\n{persona.content}\n"
            except Exception as e:
                self._log(f"获取人设失败: {e}")

        sharp_instruction = """
5. 锐评：用一句话对今天的群聊进行锐评（毒舌、幽默或温柔），语气符合人设风格。
""" if self.enable_sharp_comment else ""

        prompt = f"""你是一个群聊数据分析师。请分析以下群聊记录，生成一份简洁的日报。

{persona_prompt}

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

    async def _call_llm_incremental(self, messages: List[dict]) -> dict:
        if self._bot_self_id:
            messages = [m for m in messages if m['user_id'] != self._bot_self_id]

        msg_lines = []
        for msg in messages[-200:]:
            ts = datetime.fromtimestamp(msg["timestamp"]).strftime("%H:%M")
            user_tag = f"{msg['nickname']}#{msg['user_id']}"
            msg_lines.append(f"[{ts}] {user_tag}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        prompt = f"""请分析以下聊天记录，提取关键信息。

【重要：用户标识】
聊天记录中每个用户名字后面都有 `#QQ号` 后缀（如 `周武#123456`），即使名字相同，QQ号不同代表不同用户。请在输出中保持使用完整的 `名字#QQ号` 格式。

【要求】
1. 提取 2-3 个话题（标题+简短描述+AI吐槽）
2. 提取 2-3 条金句（发言内容+发送者，`sender` 字段必须使用 `名字#QQ号` 格式）
3. 列出发言最多的 {self.active_users_max} 位用户及发言数，`name` 字段必须使用 `名字#QQ号` 格式
4. 一句话锐评

【输出格式】（严格JSON）
{{
  "topics": [{{"title": "...", "detail": "...", "roast": "..."}}],
  "quotes": [{{"text": "...", "sender": "周武#123456"}}],
  "active_users": [{{"name": "周武#123456", "count": 0}}],
  "sharp_comment": "..."
}}

【聊天记录】
{msg_text}
"""
        return await self._call_llm(prompt)

    # ============================================================
    # 增量分析核心
    # ============================================================

    async def _do_incremental_analysis(self, group_id: str) -> bool:
        try:
            last_time = await self.db.get_last_incremental_time(group_id)
            if last_time == 0:
                last_time = int(time.time()) - self.window_hours * 3600

            messages = await self.db.get_messages_since(group_id, last_time)
            if len(messages) < self.incremental_min_messages:
                self._log(f"增量分析跳过：群 {group_id} 新增消息不足 {self.incremental_min_messages} 条")
                return False

            if len(messages) > self.max_messages_per_analysis:
                messages = messages[-self.max_messages_per_analysis:]

            result = await self._call_llm_incremental(messages)
            if not result:
                return False

            quotes = result.get('quotes', [])
            filtered_quotes = []
            for q in quotes:
                sender = q.get('sender', '')
                _, uid = parse_user_tag(sender)
                if uid and uid == self._bot_self_id:
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
                active_users=result.get('active_users', []),
                sharp_comment=result.get('sharp_comment', '')
            )

            await self.db.update_last_incremental_time(group_id, messages[-1]['timestamp'])
            self._log(f"增量分析成功：群 {group_id}，{len(messages)} 条消息")
            return True
        except Exception as e:
            logger.error(f"[KiraDaily] 增量分析失败 {group_id}: {e}")
            return False

    # ============================================================
    # 增量批次合并
    # ============================================================

    async def _merge_batches(self, group_id: str, max_batches: int = None) -> Optional[dict]:
        """
        合并增量批次
        :param group_id: 群会话ID
        :param max_batches: 最多使用最近多少个批次，None 表示不限制（全量）
        """
        cutoff = int(time.time()) - self.window_hours * 3600
        batches = await self.db.get_incremental_batches(group_id, cutoff)
        if not batches:
            return None

        if max_batches is not None and len(batches) > max_batches:
            batches = batches[-max_batches:]
            self._log(f"批次合并：使用最近 {len(batches)} 个批次")

        total_messages = sum(b['message_count'] for b in batches)
        all_users = {}
        all_topics = []
        all_quotes = []
        all_sharp_comments = []

        for batch in batches:
            try:
                topics = json.loads(batch['topics_json'])
                quotes = json.loads(batch['quotes_json'])
                users = json.loads(batch['active_users_json'])
                for u in users:
                    name_tag = u.get('name', '')
                    _, uid = parse_user_tag(name_tag)
                    if uid:
                        all_users[uid] = all_users.get(uid, 0) + u.get('count', 0)
                    else:
                        all_users[name_tag] = all_users.get(name_tag, 0) + u.get('count', 0)
                all_topics.extend(topics)
                all_quotes.extend(quotes)
                if batch.get('sharp_comment'):
                    all_sharp_comments.append(batch['sharp_comment'])
            except Exception as e:
                self._log(f"解析批次失败: {e}")

        seen_titles = set()
        unique_topics = []
        for topic in all_topics:
            if topic.get('title') and topic['title'] not in seen_titles:
                seen_titles.add(topic['title'])
                unique_topics.append(topic)

        seen_texts = set()
        unique_quotes = []
        for quote in all_quotes:
            if quote.get('text') and quote['text'] not in seen_texts:
                seen_texts.add(quote['text'])
                unique_quotes.append(quote)

        sorted_users = sorted(all_users.items(), key=lambda x: x[1], reverse=True)
        top_users = []
        for key, count in sorted_users[:self.active_users_max]:
            nickname = await self._get_nickname_by_user_id(group_id, key)
            top_users.append({
                'name': f"{nickname}#{key}",
                'count': count
            })

        return {
            'stats': {
                'total_messages': total_messages,
                'participants': len(all_users),
                'peak_hour': '00:00-23:59'
            },
            'topics': unique_topics[:self.topic_max],
            'active_users': top_users,
            'quotes': unique_quotes[:self.quote_max],
            'best_quote': None,
            'sharp_comment': all_sharp_comments[-1] if all_sharp_comments else '',
            'header_comment': f"📊 基于 {len(batches)} 个时段汇总的群聊日报"
        }

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
        now = int(time.time())
        try:
            if self.message_retention_days > 0:
                before = now - self.message_retention_days * 86400
                deleted = await self.db.delete_old_messages(before)
                if deleted:
                    self._log(f"清理了 {deleted} 条过期消息（保留 {self.message_retention_days} 天）")
            else:
                self._log("消息保留天数设为 0，永久保留")

            if self.batch_retention_days > 0:
                before = now - self.batch_retention_days * 86400
                deleted_batches = await self.db.delete_old_batches(before)
                if deleted_batches:
                    self._log(f"清理了 {deleted_batches} 个过期增量批次（保留 {self.batch_retention_days} 天）")
            else:
                self._log("增量批次保留天数设为 0，永久保留")
        except Exception as e:
            logger.error(f"[KiraDaily] 清理过期数据失败: {e}")

    # ============================================================
    # 安全执行分析
    # ============================================================

    async def _safe_do_analysis(self, group_id: str, user_id: str, immediate: bool = False):
        task_key = f"incremental_{group_id}" if immediate else f"full_{group_id}"
        try:
            if immediate:
                await self._do_incremental_analysis(group_id)
            else:
                await self._do_analysis(group_id, user_id)
        except Exception as e:
            logger.error(f"[KiraDaily] 后台分析任务失败 {group_id}: {e}")
        finally:
            self._running_analysis_tasks.discard(task_key)

    # ============================================================
    # 核心分析逻辑
    # ============================================================

    async def _do_analysis(self, group_id: str, user_id: str = "system") -> bool:
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return True

        async with self._semaphore:
            try:
                if self._is_incremental_group(group_id):
                    self._log(f"增量模式：合并群 {group_id} 的批次并补充新消息")

                    # ⭐ 1. 获取窗口内全部消息（无限制）
                    window_start_ts = int(time.time()) - self.window_hours * 3600
                    all_window_msgs = await self.db.get_messages(group_id, window_start_ts, limit=None)
                    total_messages = len(all_window_msgs)

                    # ⭐ 2. 统计真实参与人数
                    participants_count = len(set(m['user_id'] for m in all_window_msgs))

                    # ⭐ 3. 计算真实消息时间范围
                    if all_window_msgs:
                        actual_start_ts = all_window_msgs[0]['timestamp']
                        actual_end_ts = all_window_msgs[-1]['timestamp']
                    else:
                        actual_start_ts = window_start_ts
                        actual_end_ts = int(time.time())

                    # ⭐ 4. 计算活跃时段（使用统一精确算法）
                    peak_hour = self._calculate_peak_hour(all_window_msgs)

                    # ⭐ 5. 直接从原始消息统计活跃用户（准确、完整、零Token消耗）
                    user_msg_counts: Dict[str, int] = defaultdict(int)
                    user_nickname_map: Dict[str, str] = {}

                    for msg in all_window_msgs:
                        uid = msg.get('user_id', '')
                        if not uid:
                            continue
                        # 过滤 Bot 自己的消息
                        if uid == self._bot_self_id:
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

                    # 6. 获取已有批次数据并聚合（只取最近的 max_incremental_batches 个批次）
                    existing_result = await self._merge_batches(group_id, max_batches=self.max_incremental_batches)
                    topic_counts: Dict[str, int] = {}
                    topic_meta: Dict[str, dict] = {}
                    quote_counts: Dict[str, int] = {}
                    quote_meta: Dict[str, dict] = {}
                    all_sharp_comments: List[str] = []

                    if existing_result:
                        for topic in existing_result.get('topics', []):
                            title = topic.get('title', '')
                            if not title:
                                continue
                            topic_counts[title] = topic_counts.get(title, 0) + 1
                            if title not in topic_meta:
                                topic_meta[title] = topic
                        for quote in existing_result.get('quotes', []):
                            text = quote.get('text', '')
                            if not text:
                                continue
                            quote_counts[text] = quote_counts.get(text, 0) + 1
                            if text not in quote_meta:
                                quote_meta[text] = quote
                        if existing_result.get('sharp_comment'):
                            all_sharp_comments.append(existing_result['sharp_comment'])

                    # 7. 获取上次增量分析的时间点，拉取新消息
                    last_time = await self.db.get_last_incremental_time(group_id)
                    if last_time == 0:
                        self._log(f"增量模式：从未执行过增量分析，回退到全量分析")
                        return await self._do_full_analysis(group_id, user_id)

                    raw_new_messages = await self.db.get_messages_since(group_id, last_time)
                    truncated = False
                    new_messages = raw_new_messages
                    if len(raw_new_messages) > self.max_messages_per_analysis:
                        new_messages = raw_new_messages[-self.max_messages_per_analysis:]
                        truncated = True
                        self._log(f"增量模式：新增消息 {len(raw_new_messages)} 条，截断至 {len(new_messages)} 条进行分析")

                    # 8. 新增消息分析
                    if new_messages:
                        incremental_result = await self._call_llm_incremental(new_messages)
                        if incremental_result:
                            for topic in incremental_result.get('topics', []):
                                title = topic.get('title', '')
                                if not title:
                                    continue
                                topic_counts[title] = topic_counts.get(title, 0) + 1
                                if title not in topic_meta:
                                    topic_meta[title] = topic
                            for quote in incremental_result.get('quotes', []):
                                text = quote.get('text', '')
                                if not text:
                                    continue
                                quote_counts[text] = quote_counts.get(text, 0) + 1
                                if text not in quote_meta:
                                    quote_meta[text] = quote
                            if incremental_result.get('sharp_comment'):
                                all_sharp_comments.append(incremental_result['sharp_comment'])

                            await self.db.save_incremental_batch(
                                session_id=group_id,
                                start_timestamp=last_time,
                                end_timestamp=new_messages[-1]['timestamp'],
                                message_count=len(new_messages),
                                participants=len(set(m['user_id'] for m in new_messages)),
                                topics=incremental_result.get('topics', []),
                                quotes=incremental_result.get('quotes', []),
                                active_users=incremental_result.get('active_users', []),
                                sharp_comment=incremental_result.get('sharp_comment', '')
                            )
                            await self.db.update_last_incremental_time(group_id, new_messages[-1]['timestamp'])

                    # 9. 按频次排序话题和金句
                    sorted_topics = sorted(
                        topic_meta.values(),
                        key=lambda t: topic_counts.get(t.get('title', ''), 0),
                        reverse=True
                    )[:self.topic_max]

                    sorted_quotes = sorted(
                        quote_meta.values(),
                        key=lambda q: quote_counts.get(q.get('text', ''), 0),
                        reverse=True
                    )[:self.quote_max]

                    # 10. 回退检查
                    if not sorted_topics and not sorted_quotes and not active_users_from_db:
                        self._log(f"增量模式：合并后仍无数据，回退到全量分析")
                        return await self._do_full_analysis(group_id, user_id)

                    # 11. 统一润色
                    header_comment, sharp_comment, best_quote = await self._generate_final_commentary(
                        topics=sorted_topics,
                        quotes=sorted_quotes,
                        active_users=active_users_from_db,
                        existing_sharp_comments=all_sharp_comments
                    )

                    # 12. 组装最终结果
                    final_result = {
                        'stats': {
                            'total_messages': total_messages,
                            'participants': participants_count,
                            'peak_hour': peak_hour
                        },
                        'topics': sorted_topics,
                        'active_users': active_users_from_db,
                        'quotes': sorted_quotes,
                        'best_quote': best_quote,
                        'sharp_comment': sharp_comment,
                        'header_comment': header_comment
                    }

                    self._log(f"增量模式：合并完成，窗口总消息数 {total_messages}，参与人数 {participants_count}")

                    report_path = await self._generate_report(
                        group_id, final_result, [],
                        False,
                        actual_start_ts, actual_end_ts,
                        is_incremental=True
                    )
                    await self._send_report(group_id, report_path)
                    self._update_cooldown(group_id)
                    return True

                return await self._do_full_analysis(group_id, user_id)

            except Exception as e:
                logger.error(f"[KiraDaily] 分析群 {group_id} 失败: {e}")
                await self._send_text_to_group(group_id, self.msg_failed.format(error=str(e)[:100]))
                return True

    # ============================================================
    # 全量分析
    # ============================================================

    async def _do_full_analysis(self, group_id: str, user_id: str = "system") -> bool:
        messages = await self.db.get_messages(group_id, int(time.time()) - self.analysis_days * 86400)
        total_msgs = len(messages)
        if total_msgs < self.min_messages_threshold:
            await self._send_text_to_group(
                group_id,
                self.msg_too_few.format(count=total_msgs, threshold=self.min_messages_threshold)
            )
            return True

        truncated = False
        if total_msgs > self.max_messages_per_analysis:
            messages = messages[-self.max_messages_per_analysis:]
            truncated = True

        start_time = messages[0]['timestamp'] if messages else 0
        end_time = messages[-1]['timestamp'] if messages else 0

        prompt = await self._build_analysis_prompt(messages, truncated)
        result = await self._call_llm(prompt)
        if not result:
            await self._send_text_to_group(group_id, "❌ LLM 分析失败，请重试")
            return True

        quotes = result.get('quotes', [])
        filtered_quotes = []
        for q in quotes:
            sender = q.get('sender', '')
            _, uid = parse_user_tag(sender)
            if uid and uid == self._bot_self_id:
                continue
            filtered_quotes.append(q)
        result['quotes'] = filtered_quotes

        if result.get('best_quote'):
            best = result['best_quote']
            sender = best.get('sender', '')
            _, uid = parse_user_tag(sender)
            if uid and uid == self._bot_self_id:
                result['best_quote'] = None

        stats = result.get('stats', {})
        stats['total_messages'] = total_msgs
        stats['participants'] = len(set(m['user_id'] for m in messages))
        # ⭐ 覆盖 LLM 生成的 peak_hour，使用精确统计
        stats['peak_hour'] = self._calculate_peak_hour(messages)
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
            is_incremental=False
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
        is_incremental: bool = False
    ) -> Path:
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
                if uid == self._bot_self_id:
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
                avatar = await self.avatar_cache.get_user_avatar(name_tag) if name_tag.isdigit() else self._bot_avatar
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
            if uid and uid == self._bot_self_id:
                continue
            filtered_active_users.append(user)
        analysis_result['active_users'] = filtered_active_users

        for quote in analysis_result.get('quotes', []):
            sender_tag = quote.get('sender', '')
            _, uid = parse_user_tag(sender_tag)
            if uid and uid.isdigit() and sender_tag not in user_avatars:
                if uid == self._bot_self_id:
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
                if uid == self._bot_self_id:
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

        if not self._bot_avatar:
            await self._fetch_bot_info()

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

        stats['start_time'] = datetime.fromtimestamp(start_time).strftime('%m-%d %H:%M') if start_time > 0 else "未知"
        stats['end_time'] = datetime.fromtimestamp(end_time).strftime('%m-%d %H:%M') if end_time > 0 else "未知"
        analysis_result['stats'] = stats

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
            bot_avatar=self._bot_avatar or '',
            bot_nickname=self._bot_nickname or 'AI',
            generate_time=datetime.now(),
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
        adapter = self.ctx.adapter_mgr.get_adapter("qq")
        if not adapter:
            logger.error("[KiraDaily] 无法获取QQ适配器")
            return
        parts = group_id.split(":")
        if len(parts) >= 3:
            qq_group_id = parts[2]
        else:
            qq_group_id = group_id
        if self.output_format == "image":
            await adapter.send_group_message(qq_group_id, MessageChain([Image(str(report_path))]))
        else:
            text_content = report_path.read_text(encoding="utf-8")
            for chunk in [text_content[i:i+500] for i in range(0, len(text_content), 500)]:
                await adapter.send_group_message(qq_group_id, MessageChain([Text(chunk)]))
                await asyncio.sleep(0.5)

    # ============================================================
    # 定时任务
    # ============================================================

    async def _scheduler_loop(self):
        while self._running:
            try:
                now = datetime.now()
                target_time = datetime.strptime(self.auto_analysis_time, "%H:%M")
                target_datetime = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                if target_datetime <= now:
                    target_datetime += timedelta(days=1)
                wait_seconds = (target_datetime - now).total_seconds()
                self._log(f"距离下次自动日报还有 {wait_seconds:.0f} 秒")
                await asyncio.sleep(wait_seconds)
                if not self._running:
                    break
                await self._run_auto_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[KiraDaily] 定时任务异常: {e}")
                await asyncio.sleep(60)

    async def _run_auto_analysis(self):
        self._log("开始执行自动日报分析")
        await self._cleanup_old_messages()

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
        tasks = []
        for group_id in groups:
            tasks.append(self._do_incremental_analysis(group_id))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        self._log(f"增量分析完成: {success_count}/{len(groups)} 群成功")

    # ============================================================
    # 命令处理
    # ============================================================

    @on.im_message(priority=Priority.MEDIUM)
    async def handle_command(self, event: KiraMessageEvent):
        if not self.enable_command_trigger or not event.is_group_message():
            return
        text = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text)).strip()
        if text not in self.command_prefixes:
            return
        group_id = self._get_group_id_from_event(event)
        if not group_id:
            await self.ctx.message_processor.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_group)])
            )
            event.discard(force=True)
            event.stop()
            return
        if not self._is_group_enabled(group_id):
            await self.ctx.message_processor.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_enabled)])
            )
            event.discard(force=True)
            event.stop()
            return
        user_id = self._get_user_id_from_event(event)
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            event.discard(force=True)
            event.stop()
            return
        await self.ctx.message_processor.send_message_chain(
            session=self._get_sid(event),
            chain=MessageChain([Text(self.msg_processing)])
        )
        await self._do_analysis(group_id, user_id)
        event.discard(force=True)
        event.stop()

    # ============================================================
    # LLM 工具
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
        original_days = self.analysis_days
        self.analysis_days = min(max(days, 1), 7)
        user_id = self._get_user_id_from_event(event)
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            self.analysis_days = original_days
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return "✅ 冷却中，已向用户说明。"
        await self._do_analysis(group_id, user_id)
        self.analysis_days = original_days
        return "✅ 日报任务已完成处理，结果已发送到群聊。"

    # ============================================================
    # 清理旧报告
    # ============================================================

    async def _cleanup_old_reports(self):
        try:
            files = list(self.reports_dir.glob("*.png")) + list(self.reports_dir.glob("*.html")) + list(self.reports_dir.glob("*.txt"))
            if not files:
                return
            files.sort(key=lambda f: f.stat().st_mtime)
            if len(files) > self.report_cleanup_count:
                to_delete = files[:self.report_cleanup_count]
                for f in to_delete:
                    try:
                        f.unlink(missing_ok=True)
                        meta_path = f.with_suffix(".meta.json")
                        meta_path.unlink(missing_ok=True)
                    except Exception as e:
                        self._log(f"删除旧报告失败 {f}: {e}")
                self._log(f"清理了 {len(to_delete)} 个旧报告")
        except Exception as e:
            logger.error(f"[KiraDaily] 清理报告失败: {e}")
