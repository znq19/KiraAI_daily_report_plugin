import base64
import os
import time
from pathlib import Path
from typing import Optional
import aiohttp
import aiofiles

class AvatarCache:
    """QQ头像缓存管理器（支持过期自动刷新）"""
    
    USER_AVATAR_URL = "https://q.qlogo.cn/g?b=qq&nk={user_id}&s=640"
    GROUP_AVATAR_URL = "https://p.qlogo.cn/gh/{group_id}/{group_id}/0/"
    BOT_AVATAR_URL = "https://q.qlogo.cn/headimg_dl?dst_uin={self_id}&spec=640"
    
    def __init__(self, cache_dir: Path, expire_days: int = 3):
        self.cache_dir = cache_dir
        self.expire_days = expire_days
        self.expire_seconds = expire_days * 86400 if expire_days > 0 else 0
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    async def init(self):
        """初始化"""
        pass
    
    def _get_cache_path(self, key: str, avatar_type: str = "user") -> Path:
        """获取缓存路径"""
        sub_dir = self.cache_dir / avatar_type
        sub_dir.mkdir(parents=True, exist_ok=True)
        return sub_dir / f"{key}.jpg"
    
    async def _download_and_cache(self, url: str, cache_path: Path) -> Optional[str]:
        """下载并缓存图片，返回Base64"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        async with aiofiles.open(cache_path, "wb") as f:
                            await f.write(data)
                        return base64.b64encode(data).decode()
                    return None
        except Exception:
            return None
    
    async def _get_avatar(self, key: str, url_template: str, avatar_type: str, force_refresh: bool = False) -> str:
        """通用获取头像方法，支持过期检查"""
        cache_path = self._get_cache_path(key, avatar_type)
        
        # 检查缓存是否有效
        if not force_refresh and cache_path.exists():
            # 如果 expire_seconds == 0，永不过期
            if self.expire_seconds == 0:
                data = cache_path.read_bytes()
                return base64.b64encode(data).decode()
            # 检查文件修改时间
            age = time.time() - cache_path.stat().st_mtime
            if age < self.expire_seconds:
                data = cache_path.read_bytes()
                return base64.b64encode(data).decode()
            else:
                # 过期，删除旧文件
                cache_path.unlink(missing_ok=True)
        
        # 下载新的
        url = url_template.format(user_id=key, group_id=key, self_id=key)
        b64 = await self._download_and_cache(url, cache_path)
        if b64:
            return b64
        else:
            return self._get_default_avatar()
    
    async def get_user_avatar(self, user_id: str, force_refresh: bool = False) -> str:
        """获取用户头像Base64"""
        return await self._get_avatar(user_id, self.USER_AVATAR_URL, "user", force_refresh)
    
    async def get_group_avatar(self, group_id: str, force_refresh: bool = False) -> str:
        """获取群头像Base64"""
        if ":" in group_id:
            parts = group_id.split(":")
            group_id = parts[-1]
        return await self._get_avatar(group_id, self.GROUP_AVATAR_URL, "group", force_refresh)
    
    async def get_bot_avatar(self, self_id: str, force_refresh: bool = False) -> str:
        """获取Bot头像Base64"""
        return await self._get_avatar(self_id, self.BOT_AVATAR_URL, "bot", force_refresh)
    
    def _get_default_avatar(self) -> str:
        """返回默认头像Base64（1x1透明像素）"""
        return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="