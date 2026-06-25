import asyncio
import sys
from pathlib import Path
from typing import Optional
from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.async_api import async_playwright


async def find_or_install_browser(prefer_system: bool = True):
    """
    检测系统中可用的 Chrome/Chromium/Edge 浏览器。
    若 prefer_system=False，则直接下载内置 Chromium。
    """
    if not prefer_system:
        print("[KiraDaily] 配置强制使用内置 Chromium，跳过系统浏览器检测")
        return await _install_chromium()

    candidates = [
        ("chrome", "Google Chrome"),
        ("msedge", "Microsoft Edge"),
        ("chromium", "Chromium"),
    ]

    for channel, display_name in candidates:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(channel=channel, headless=True)
                await browser.close()
            print(f"[KiraDaily] 检测到系统浏览器: {display_name}")
            return channel
        except Exception:
            continue

    print("[KiraDaily] 未检测到系统浏览器，将下载内置 Chromium...")
    return await _install_chromium()


async def _install_chromium():
    """下载并安装内置 Chromium"""
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "playwright", "install", "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(stderr.decode() if stderr else f"退出码 {proc.returncode}")
        print("[KiraDaily] Chromium 下载完成。")
    except Exception as e:
        raise RuntimeError(
            f"Chromium 自动下载失败: {e}\n"
            f"请尝试手动运行: {sys.executable} -m playwright install chromium"
        )

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            await browser.close()
        print("[KiraDaily] Chromium 安装验证通过。")
    except Exception as e:
        raise RuntimeError(
            f"Chromium 安装后仍无法启动: {e}\n"
            f"当前系统可能缺少必要的运行时库。"
        )

    return None


class HTMLRenderer:
    """HTML渲染器（基于Playwright）"""
    
    def __init__(self, data_dir: Path, prefer_system_browser: bool = True, render_timeout: int = 30):
        self.template_dir = Path(__file__).parent.parent / "templates"
        self.template_dir.mkdir(parents=True, exist_ok=True)
        self._browser_ready = asyncio.Event()
        self._browser_channel = None
        self._prefer_system = prefer_system_browser
        self._render_timeout = render_timeout * 1000  # 转为毫秒
        
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(['html', 'xml'])
        )
        
        asyncio.create_task(self._init_browser())
    
    async def _init_browser(self):
        try:
            channel = await find_or_install_browser(self._prefer_system)
            self._browser_channel = channel
            if channel:
                print(f"[KiraDaily] 将使用系统浏览器 (channel={channel}) 渲染截图。")
            else:
                print("[KiraDaily] 将使用 Playwright 内置 Chromium 渲染截图。")
        except Exception as e:
            print(f"[KiraDaily] 浏览器初始化失败: {e}")
        finally:
            self._browser_ready.set()
    
    def render_template(self, **kwargs) -> str:
        template = self.jinja_env.get_template("daily_report.html")
        return template.render(**kwargs)
    
    async def render_to_image(self, html_content: str, output_path: str) -> bool:
        await self._browser_ready.wait()
        
        try:
            async with async_playwright() as p:
                launch_kwargs = {"headless": True}
                if self._browser_channel:
                    launch_kwargs["channel"] = self._browser_channel
                
                browser = await p.chromium.launch(**launch_kwargs)
                page = await browser.new_page(viewport={"width": 800, "height": 1200})
                
                await page.set_content(
                    html_content,
                    wait_until="networkidle",
                    timeout=self._render_timeout
                )
                
                height = await page.evaluate("document.body.scrollHeight")
                await page.set_viewport_size({"width": 800, "height": height + 100})
                
                await page.screenshot(path=output_path, full_page=True)
                await browser.close()
                return True
        except Exception as e:
            print(f"[KiraDaily] HTML渲染失败: {e}")
            return False