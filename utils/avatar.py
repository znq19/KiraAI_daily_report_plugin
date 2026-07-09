import asyncio
import base64
import time
from pathlib import Path
from typing import Optional
import aiohttp
import aiofiles
# 导入需要的 resolver
from aiohttp.resolver import ThreadedResolver

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

    async def _download_and_cache(self, url: str, cache_path: Path, retry: int = 1) -> Optional[str]:
        """
        下载并缓存图片，返回Base64
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://q.qlogo.cn/",
        }
        
        # ★★★ 核心修改：显式指定使用 ThreadedResolver ★★★
        # 这能解决在 Windows 等环境下 aiohttp 异步 DNS 解析失败的问题
        resolver = ThreadedResolver()
        connector = aiohttp.TCPConnector(
            resolver=resolver,
            ssl=False # 保持 SSL 验证关闭，解决可能的证书问题
        )

        for attempt in range(retry + 1):
            try:
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, timeout=10, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) < 100:
                                print(f"[KiraDaily] 头像数据过小 ({len(data)} 字节)，可能无效: {url}")
                                return None
                            async with aiofiles.open(cache_path, "wb") as f:
                                await f.write(data)
                            return base64.b64encode(data).decode()
                        else:
                            print(f"[KiraDaily] 头像下载失败 HTTP {resp.status}: {url}")
                            return None
            except Exception as e:
                print(f"[KiraDaily] 头像下载异常 (尝试 {attempt+1}/{retry+1}): {e} (URL: {url})")
                if attempt < retry:
                    await asyncio.sleep(1)
                    continue
                else:
                    return None
        return None

    async def _get_avatar(self, key: str, url_template: str, avatar_type: str, force_refresh: bool = False) -> str:
        """通用获取头像方法，支持过期检查"""
        cache_path = self._get_cache_path(key, avatar_type)

        if not force_refresh and cache_path.exists():
            if self.expire_seconds == 0:
                try:
                    data = cache_path.read_bytes()
                    return base64.b64encode(data).decode()
                except Exception as e:
                    print(f"[KiraDaily] 读取缓存失败: {e}，将重新下载")
                    cache_path.unlink(missing_ok=True)
            else:
                age = time.time() - cache_path.stat().st_mtime
                if age < self.expire_seconds:
                    try:
                        data = cache_path.read_bytes()
                        return base64.b64encode(data).decode()
                    except Exception as e:
                        print(f"[KiraDaily] 读取缓存失败: {e}，将重新下载")
                        cache_path.unlink(missing_ok=True)
                else:
                    cache_path.unlink(missing_ok=True)

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
