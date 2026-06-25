import time
from pathlib import Path
from typing import List, Dict, Optional
import aiosqlite
import json

class DatabaseManager:
    """消息数据库管理（含增量分析支持）"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
    
    async def init(self):
        """初始化连接"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
    
    async def close(self):
        """关闭连接"""
        if self._conn:
            await self._conn.close()
    
    async def create_tables(self):
        """创建所有表"""
        # 消息表
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                nickname TEXT,
                content TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                platform TEXT DEFAULT 'qq'
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_session_time 
            ON messages(session_id, timestamp)
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_timestamp
            ON messages(timestamp)
        """)
        
        # 增量分析批次表
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS incremental_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                start_timestamp INTEGER NOT NULL,
                end_timestamp INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                participants INTEGER NOT NULL,
                topics_json TEXT NOT NULL,
                quotes_json TEXT NOT NULL,
                active_users_json TEXT NOT NULL,
                sharp_comment TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_batch_session_time 
            ON incremental_batches(session_id, start_timestamp)
        """)
        
        # 增量分析状态表（记录每个群的上次分析时间）
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS incremental_state (
                session_id TEXT PRIMARY KEY,
                last_timestamp INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        
        await self._conn.commit()
    
    # ============================================================
    # 消息 CRUD
    # ============================================================
    
    async def save_message(self, session_id: str, user_id: str, nickname: str, 
                          content: str, timestamp: int):
        """保存消息"""
        await self._conn.execute(
            "INSERT INTO messages (session_id, user_id, nickname, content, timestamp) VALUES (?, ?, ?, ?, ?)",
            (session_id, user_id, nickname, content, timestamp)
        )
        await self._conn.commit()
    
    async def get_messages(self, session_id: str, since_timestamp: int, limit: int = 500) -> List[dict]:
        """获取消息（按时间正序）"""
        cursor = await self._conn.execute(
            "SELECT user_id, nickname, content, timestamp FROM messages "
            "WHERE session_id = ? AND timestamp >= ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (session_id, since_timestamp, limit)
        )
        rows = await cursor.fetchall()
        # 反转成正序
        return [
            {"user_id": r[0], "nickname": r[1], "content": r[2], "timestamp": r[3]}
            for r in rows[::-1]
        ]
    
    async def get_messages_since(self, session_id: str, since_timestamp: int) -> List[dict]:
        """获取指定时间之后的所有消息（不限量，用于增量分析）"""
        cursor = await self._conn.execute(
            "SELECT user_id, nickname, content, timestamp FROM messages "
            "WHERE session_id = ? AND timestamp > ? "
            "ORDER BY timestamp ASC",
            (session_id, since_timestamp)
        )
        rows = await cursor.fetchall()
        return [
            {"user_id": r[0], "nickname": r[1], "content": r[2], "timestamp": r[3]}
            for r in rows
        ]
    
    async def get_all_groups(self) -> List[str]:
        """获取所有出现过消息的群"""
        cursor = await self._conn.execute(
            "SELECT DISTINCT session_id FROM messages WHERE session_id LIKE 'qq:gm:%'"
        )
        rows = await cursor.fetchall()
        return [r[0] for r in rows]
    
    async def delete_old_messages(self, before_timestamp: int):
        """删除旧消息"""
        await self._conn.execute(
            "DELETE FROM messages WHERE timestamp < ?",
            (before_timestamp,)
        )
        await self._conn.commit()
    
    # ============================================================
    # 增量分析状态管理
    # ============================================================
    
    async def get_last_incremental_time(self, session_id: str) -> int:
        """获取上次增量分析的时间戳"""
        cursor = await self._conn.execute(
            "SELECT last_timestamp FROM incremental_state WHERE session_id = ?",
            (session_id,)
        )
        row = await cursor.fetchone()
        if row:
            return row[0]
        return 0
    
    async def update_last_incremental_time(self, session_id: str, timestamp: int):
        """更新上次增量分析的时间戳"""
        await self._conn.execute(
            "INSERT OR REPLACE INTO incremental_state (session_id, last_timestamp, updated_at) VALUES (?, ?, ?)",
            (session_id, timestamp, int(time.time()))
        )
        await self._conn.commit()
    
    # ============================================================
    # 增量批次管理
    # ============================================================
    
    async def save_incremental_batch(
        self,
        session_id: str,
        start_timestamp: int,
        end_timestamp: int,
        message_count: int,
        participants: int,
        topics: List[dict],
        quotes: List[dict],
        active_users: List[dict],
        sharp_comment: str = ''
    ):
        """保存增量分析批次"""
        await self._conn.execute(
            """
            INSERT INTO incremental_batches (
                session_id, start_timestamp, end_timestamp, message_count, participants,
                topics_json, quotes_json, active_users_json, sharp_comment, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                start_timestamp,
                end_timestamp,
                message_count,
                participants,
                json.dumps(topics, ensure_ascii=False),
                json.dumps(quotes, ensure_ascii=False),
                json.dumps(active_users, ensure_ascii=False),
                sharp_comment,
                int(time.time())
            )
        )
        await self._conn.commit()
    
    async def get_incremental_batches(self, session_id: str, since_timestamp: int) -> List[dict]:
        """获取指定时间之后的增量批次（按时间正序）"""
        cursor = await self._conn.execute(
            """
            SELECT id, start_timestamp, end_timestamp, message_count, participants,
                   topics_json, quotes_json, active_users_json, sharp_comment, created_at
            FROM incremental_batches
            WHERE session_id = ? AND start_timestamp >= ?
            ORDER BY start_timestamp ASC
            """,
            (session_id, since_timestamp)
        )
        rows = await cursor.fetchall()
        return [
            {
                'id': r[0],
                'start_timestamp': r[1],
                'end_timestamp': r[2],
                'message_count': r[3],
                'participants': r[4],
                'topics_json': r[5],
                'quotes_json': r[6],
                'active_users_json': r[7],
                'sharp_comment': r[8],
                'created_at': r[9]
            }
            for r in rows
        ]
    
    async def delete_old_batches(self, before_timestamp: int):
        """删除旧的增量批次"""
        await self._conn.execute(
            "DELETE FROM incremental_batches WHERE created_at < ?",
            (before_timestamp,)
        )
        await self._conn.commit()