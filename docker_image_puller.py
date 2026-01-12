import os
import sys
import gzip
import json
import hashlib
import shutil
import threading
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import tarfile
import urllib3
import argparse
import logging
import base64
import zstandard as zstd
import platform

from concurrent.futures import ThreadPoolExecutor, as_completed

# Set default encoding to UTF-8
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 禁用 SSL 警告
urllib3.disable_warnings()

# 版本号
VERSION = "v1.2.0"

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', encoding='utf-8')
logger = logging.getLogger(__name__)

stop_event = threading.Event()


def create_session():
    """创建带有重试和代理配置的请求会话"""
    session = requests.Session()

    # 增强重试策略：更多重试次数，更长超时
    retry_strategy = Retry(
        total=5,  # 增加重试次数
        backoff_factor=2,  # 指数退避：2, 4, 8, 16, 32秒
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"]
    )

    # 设置HTTPAdapter，更长超时
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,  # 连接池大小
        pool_maxsize=20,  # 最大连接数
        pool_block=False
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # 设置默认超时
    session.timeout = (30, 300)  # (连接超时, 读取超时)

    # 设置代理
    session.proxies = {
        'http': os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy'),
        'https': os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
    }
    if session.proxies.get('http') or session.proxies.get('https'):
        logger.info('使用代理设置从环境变量')

    return session


def parse_image_input(args):
    """解析用户输入的镜像名称，支持私有仓库格式"""
    image_input = args.image
    # 检查是否包含私有仓库地址
    if '/' in image_input and ('.' in image_input.split('/')[0] or ':' in image_input.split('/')[0]):
        # 私有仓库格式: harbor.abc.com/abc/nginx:1.26.0
        registry, remainder = image_input.split('/', 1)
        parts = remainder.split('/')
        if len(parts) == 1:
            repo = ''
            img_tag = parts[0]
        else:
            repo = '/'.join(parts[:-1])
            img_tag = parts[-1]

        # 解析镜像名和标签
        img, *tag_parts = img_tag.split(':')
        tag = tag_parts[0] if tag_parts else 'latest'

        # 组合成完整的仓库路径
        repository = remainder.split(':')[0]

        return registry, repository, img, tag
    else:
        # 标准Docker Hub格式
        parts = image_input.split('/')
        if len(parts) == 1:
            repo = 'library'
            img_tag = parts[0]
        else:
            repo = '/'.join(parts[:-1])
            img_tag = parts[-1]

        # 解析镜像名和标签
        img, *tag_parts = img_tag.split(':')
        tag = tag_parts[0] if tag_parts else 'latest'

        # 组合成完整的仓库路径
        repository = f'{repo}/{img}'
        if not args.custom_registry:
            registry = 'registry-1.docker.io'
        else:
            registry = args.custom_registry
        return registry, repository, img, tag


def get_auth_head(session, auth_url, reg_service, repository, username=None, password=None):
    """获取认证头，支持用户名密码认证"""
    try:
        url = f'{auth_url}?service={reg_service}&scope=repository:{repository}:pull'

        headers = {}
        # 如果提供了用户名和密码，添加到请求头
        if username and password:
            auth_string = f"{username}:{password}"
            encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
            headers['Authorization'] = f'Basic {encoded_auth}'

        # 打印 curl 命令
        logger.debug(f"获取认证头 CURL 命令: curl '{url}'")

        resp = session.get(url, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
        access_token = resp.json()['token']
        auth_head = {'Authorization': f'Bearer {access_token}',
                     'Accept': 'application/vnd.docker.distribution.manifest.v2+json'}

        return auth_head
    except requests.exceptions.RequestException as e:
        logger.error(f'请求认证失败: {e}')
        raise


def fetch_manifest(session, registry, repository, tag, auth_head):
    """获取镜像清单"""
    try:
        url = f'https://{registry}/v2/{repository}/manifests/{tag}'
        # 打印 curl 命令
        headers = ' '.join([f"-H '{key}: {value}'" for key, value in auth_head.items()])
        curl_command = f"curl '{url}' {headers}"
        logger.debug(f'获取镜像清单 CURL 命令: {curl_command}')
        resp = session.get(url, headers=auth_head, verify=False, timeout=30)
        if resp.status_code == 401:
            logger.info('需要认证。')
            return resp, 401
        resp.raise_for_status()
        return resp, 200
    except requests.exceptions.RequestException as e:
        logger.error(f'请求清单失败: {e}')
        raise


def select_manifest(manifests, arch):
    """选择适合指定架构的清单"""
    selected_manifest = None
    for m in manifests:
        if (m.get('annotations', {}).get('com.docker.official-images.bashbrew.arch') == arch or \
            m.get('platform', {}).get('architecture') == arch) and \
                m.get('platform', {}).get('os') == 'linux':
            selected_manifest = m.get('digest')
            break
    return selected_manifest


class DownloadProgressManager:
    """下载进度管理器，支持进度持久化"""

    def __init__(self, repository, tag, arch):
        """
        初始化进度管理器

        Args:
            repository: 镜像仓库名（如：library/alpine）
            tag: 镜像标签（如：latest）
            arch: 架构（如：amd64）
        """
        self.repository = repository
        self.tag = tag
        self.arch = arch

        # 生成唯一的进度文件名
        safe_repo = repository.replace("/", "_").replace(":", "_")
        self.progress_file = f'.download_progress_{safe_repo}_{tag}_{arch}.json'

        self.progress_data = self.load_progress()

    def load_progress(self):
        """加载下载进度，并验证镜像信息"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    # 验证镜像信息是否匹配
                    metadata = data.get('metadata', {})
                    if (metadata.get('repository') == self.repository and
                            metadata.get('tag') == self.tag and
                            metadata.get('arch') == self.arch):

                        logger.info(f'📋 加载已有下载进度，共 {len(data.get("layers", {}))} 个文件')
                        return data
                    else:
                        logger.warning(f'进度文件镜像信息不匹配，将创建新的进度')
                        logger.debug(
                            f'文件中: {metadata}, 当前: {{repository: {self.repository}, tag: {self.tag}, arch: {self.arch}}}')
                        # 镜像信息不匹配，返回新的进度数据
                        return self._create_new_progress()

            except Exception as e:
                logger.warning(f'加载进度文件失败: {e}')

        return self._create_new_progress()

    def _create_new_progress(self):
        """创建新的进度数据结构"""
        return {
            'metadata': {
                'repository': self.repository,
                'tag': self.tag,
                'arch': self.arch,
                'created_at': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'layers': {},
            'config': None
        }

    def save_progress(self):
        """保存下载进度"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f'保存进度文件失败: {e}')

    def update_layer_status(self, digest, status, **kwargs):
        """更新层的下载状态

        Args:
            digest: 层的digest
            status: 状态 (pending/downloading/completed/failed)
            **kwargs: 其他信息（size, downloaded等）
        """
        if digest not in self.progress_data['layers']:
            self.progress_data['layers'][digest] = {}

        self.progress_data['layers'][digest]['status'] = status
        self.progress_data['layers'][digest].update(kwargs)
        self.save_progress()

    def get_layer_status(self, digest):
        """获取层的下载状态"""
        return self.progress_data['layers'].get(digest, {})

    def is_layer_completed(self, digest):
        """检查层是否已完成下载"""
        layer_info = self.get_layer_status(digest)
        return layer_info.get('status') == 'completed'

    def update_config_status(self, status, **kwargs):
        """更新配置文件状态"""
        if self.progress_data['config'] is None:
            self.progress_data['config'] = {}
        self.progress_data['config']['status'] = status
        self.progress_data['config'].update(kwargs)
        self.save_progress()

    def is_config_completed(self):
        """检查配置文件是否已完成下载"""
        config_data = self.progress_data.get('config')
        if config_data is None:
            return False
        return config_data.get('status') == 'completed'

    def clear_progress(self):
        """清除进度文件"""
        if os.path.exists(self.progress_file):
            try:
                os.remove(self.progress_file)
                logger.debug('进度文件已清除')
            except Exception as e:
                logger.error(f'清除进度文件失败: {e}')


def download_file_with_progress(session, url, headers, save_path, desc, expected_digest=None, max_retries=5):
    """
    下载文件，支持断点续传、重试和校验

    Args:
        session: 请求会话
        url: 下载URL
        headers: 请求头
        save_path: 保存路径
        desc: 进度条描述
        expected_digest: 期望的SHA256摘要（可选）
        max_retries: 最大重试次数

    Returns:
        bool: 下载是否成功
    """
    for attempt in range(max_retries):
        if stop_event.is_set():
            logger.info('下载被用户取消')
            return False

        # 每次重试时重新计算已下载的大小
        resume_pos = 0
        if os.path.exists(save_path):
            resume_pos = os.path.getsize(save_path)
            if resume_pos > 0:
                logger.info(f'{desc}: 断点续传，从位置 {resume_pos} 开始')

        # 添加Range头实现断点续传
        download_headers = headers.copy()
        if resume_pos > 0:
            download_headers['Range'] = f'bytes={resume_pos}-'

        try:
            with session.get(url, headers=download_headers, verify=False, timeout=60, stream=True) as resp:
                resp.raise_for_status()

                # 获取总大小
                content_range = resp.headers.get('content-range')
                if content_range:
                    total_size = int(content_range.split('/')[1])
                else:
                    total_size = int(resp.headers.get('content-length', 0)) + resume_pos

                # 检查是否需要续传
                mode = 'ab' if resume_pos > 0 else 'wb'

                # 初始化SHA256计算器
                sha256_hash = hashlib.sha256() if expected_digest else None

                # 如果是断点续传且需要校验，先读取已下载部分计算哈希
                if resume_pos > 0 and sha256_hash:
                    logger.debug(f'{desc}: 计算已下载部分的校验和...')
                    with open(save_path, 'rb') as existing_file:
                        while True:
                            chunk = existing_file.read(8192)
                            if not chunk:
                                break
                            sha256_hash.update(chunk)

                with open(save_path, mode) as file, tqdm(
                        total=total_size, initial=resume_pos, unit='B', unit_scale=True,
                        desc=desc, position=0, leave=True
                ) as pbar:
                    downloaded_size = resume_pos

                    for chunk in resp.iter_content(chunk_size=8192):
                        if stop_event.is_set():
                            logger.info('下载被用户取消')
                            return False

                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
                            downloaded_size += len(chunk)

                            # 实时计算哈希（包含新下载的部分）
                            if sha256_hash:
                                sha256_hash.update(chunk)

                # 下载完成，验证哈希
                if expected_digest and sha256_hash:
                    actual_digest = f'sha256:{sha256_hash.hexdigest()}'
                    if actual_digest != expected_digest:
                        logger.error(f'❌ {desc} 校验失败！')
                        logger.error(f'期望: {expected_digest}')
                        logger.error(f'实际: {actual_digest}')
                        logger.info(f'删除损坏文件，准备重新下载...')

                        # 删除损坏的文件并重试
                        if os.path.exists(save_path):
                            os.remove(save_path)

                        # 等待后重试
                        if attempt < max_retries - 1:
                            wait_time = (2 ** attempt)
                            logger.info(f'等待 {wait_time} 秒后重试...')
                            time.sleep(wait_time)
                        continue  # 重试

                    logger.info(f'✅ {desc} 校验成功')

                logger.info(f'✅ {desc} 下载完成')
                return True

        except KeyboardInterrupt:
            logger.info(f'⚠️  下载 {url} 被用户取消')
            if os.path.exists(save_path):
                os.remove(save_path)
            return False
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f'⚠️  {desc} 第 {attempt + 1}/{max_retries} 次下载超时: {e}')
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt)  # 指数退避：2, 4, 8, 16, 32秒
                logger.info(f'等待 {wait_time} 秒后重试...')
                time.sleep(wait_time)
                continue
            else:
                logger.error(f'❌ {desc} 下载失败，已达到最大重试次数')
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False
        except requests.exceptions.HTTPError as e:
            logger.warning(f'⚠️  {desc} HTTP错误: {e}')
            if e.response.status_code in [429, 500, 502, 503, 504] and attempt < max_retries - 1:
                wait_time = (2 ** attempt)
                logger.info(f'等待 {wait_time} 秒后重试...')
                time.sleep(wait_time)
                continue
            else:
                logger.error(f'❌ {desc} 下载失败: {e}')
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False
        except Exception as e:
            logger.error(f'❌ {desc} 下载失败: {e}')
            if os.path.exists(save_path):
                os.remove(save_path)
            return False

    return False


def download_layers(session, registry, repository, layers, auth_head, imgdir, resp_json, imgparts, img, tag, arch):
    """多线程下载镜像层，支持断点续传、重试和校验"""
    os.makedirs(imgdir, exist_ok=True)

    # 创建进度管理器（每个镜像独立的进度文件）
    progress_manager = DownloadProgressManager(repository, tag, arch)

    try:
        config_digest = resp_json['config']['digest']
        config_filename = f'{config_digest[7:]}.json'
        config_path = os.path.join(imgdir, config_filename)
        config_url = f'https://{registry}/v2/{repository}/blobs/{config_digest}'

        # 检查配置文件是否已下载完成
        if progress_manager.is_config_completed() and os.path.exists(config_path):
            logger.info(f'✅ Config {config_filename} 已存在，跳过下载')
        else:
            logger.debug(f'下载 Config: {config_filename}')
            progress_manager.update_config_status('downloading', digest=config_digest)

            # 下载配置文件并进行校验
            if not download_file_with_progress(session, config_url, auth_head, config_path, "Config",
                                               expected_digest=config_digest):
                progress_manager.update_config_status('failed')
                raise Exception(f'Config JSON {config_filename} 下载失败')

            progress_manager.update_config_status('completed', digest=config_digest)

    except Exception as e:
        logging.error(f'请求配置失败: {e}')
        return

    repo_tag = f'{"/".join(imgparts)}/{img}:{tag}' if imgparts else f'{img}:{tag}'
    content = [{'Config': config_filename, 'RepoTags': [repo_tag], 'Layers': []}]
    parentid = ''
    layer_json_map = {}

    # 统计需要下载的层
    layers_to_download = []
    skipped_count = 0

    for layer in layers:
        ublob = layer['digest']
        fake_layerid = hashlib.sha256((parentid + '\n' + ublob + '\n').encode('utf-8')).hexdigest()
        layerdir = f'{imgdir}/{fake_layerid}'
        os.makedirs(layerdir, exist_ok=True)
        layer_json_map[fake_layerid] = {"id": fake_layerid, "parent": parentid if parentid else None}
        parentid = fake_layerid

        save_path = f'{layerdir}/layer_gzip.tar'

        # 检查是否已完成下载
        if progress_manager.is_layer_completed(ublob) and os.path.exists(save_path):
            logger.info(f'✅ 层 {ublob[:12]} 已存在，跳过下载')
            skipped_count += 1
        else:
            layers_to_download.append((ublob, fake_layerid, layerdir, save_path))

    if skipped_count > 0:
        logger.info(f'📦 跳过 {skipped_count} 个已下载的层，还需下载 {len(layers_to_download)} 个层')

    # 多线程下载需要下载的层
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        try:
            for ublob, fake_layerid, layerdir, save_path in layers_to_download:
                if stop_event.is_set():
                    raise KeyboardInterrupt  # 检测到终止信号

                url = f'https://{registry}/v2/{repository}/blobs/{ublob}'

                # 标记为下载中
                progress_manager.update_layer_status(ublob, 'downloading')

                # 传递digest进行校验
                futures[executor.submit(
                    download_file_with_progress,
                    session,
                    url,
                    auth_head,
                    save_path,
                    ublob[:12],
                    expected_digest=ublob
                )] = (ublob, save_path)

            for future in as_completed(futures):
                if stop_event.is_set():
                    raise KeyboardInterrupt  # 退出

                ublob, save_path = futures[future]
                result = future.result()

                if not result:
                    progress_manager.update_layer_status(ublob, 'failed')
                    raise Exception(f'层 {ublob[:12]} 下载失败')
                else:
                    progress_manager.update_layer_status(ublob, 'completed')

        except KeyboardInterrupt:
            logging.error("用户终止下载，保存当前进度...")
            stop_event.set()  # 设置终止标志
            executor.shutdown(wait=False)
            # 不删除部分下载的文件，保留用于断点续传
            raise  # 重新抛出异常，让外层处理

    # 解压和处理所有层
    for fake_layerid in layer_json_map.keys():
        if stop_event.is_set():
            # 检测到终止信号，提前退出
            raise KeyboardInterrupt("用户已取消操作")

        layerdir = f'{imgdir}/{fake_layerid}'
        gz_path = f'{layerdir}/layer_gzip.tar'
        tar_path = f'{layerdir}/layer.tar'

        # 解压gzip文件
        # 解压文件（支持 gzip 和 zstd）
        if os.path.exists(gz_path):
            try:
                # 尝试读取文件头来判断格式
                with open(gz_path, 'rb') as f:
                    magic = f.read(4)

                # 判断是否为 zstd (Magic number: 0x28 0xB5 0x2F 0xFD)
                if magic.startswith(b'\x28\xb5\x2f\xfd'):
                    logger.info(f"检测到 Zstd 压缩: {gz_path}")
                    dctx = zstd.ZstdDecompressor()
                    with open(gz_path, 'rb') as ifh, open(tar_path, 'wb') as ofh:
                        dctx.copy_stream(ifh, ofh)
                else:
                    # 默认为 gzip
                    with gzip.open(gz_path, 'rb') as gz, open(tar_path, 'wb') as file:
                        shutil.copyfileobj(gz, file)

                os.remove(gz_path)
            except Exception as e:
                logger.error(f"解压失败: {e}")
                # 这里可以选择是否抛出异常或继续
                raise

        json_path = f'{layerdir}/json'
        with open(json_path, 'w') as file:
            json.dump(layer_json_map[fake_layerid], file)

        content[0]['Layers'].append(f'{fake_layerid}/layer.tar')

    manifest_path = os.path.join(imgdir, 'manifest.json')
    with open(manifest_path, 'w') as file:
        json.dump(content, file)

    repositories_path = os.path.join(imgdir, 'repositories')
    with open(repositories_path, 'w') as file:
        json.dump({repository if '/' in repository else img: {tag: parentid}}, file)

    logging.info(f'✅ 镜像 {img}:{tag} 下载完成！')

    # 清除进度文件
    progress_manager.clear_progress()


def create_image_tar(imgdir, repository, tag, arch):
    """将镜像打包为 tar.gz 文件"""
    safe_repo = repository.replace("/", "_")
    # 修改文件名后缀为 .tar.gz
    docker_tar = f'{safe_repo}_{tag}_{arch}.tar.gz'
    try:
        # 修改模式为 "w:gz" 以启用 gzip 压缩
        # compresslevel=6 是默认值，平衡速度和压缩率。如果想要更小体积但更慢，可以设为 9
        with tarfile.open(docker_tar, "w:gz", compresslevel=6) as tar:
            tar.add(imgdir, arcname='/')
        logger.debug(f'Docker 镜像已压缩打包：{docker_tar}')
        return docker_tar
    except Exception as e:
        logger.error(f'打包镜像失败: {e}')
        raise



def cleanup_tmp_dir():
    """删除 tmp 目录"""
    tmp_dir = 'tmp'
    try:
        if os.path.exists(tmp_dir):
            logger.debug(f'清理临时目录: {tmp_dir}')
            shutil.rmtree(tmp_dir)
            logger.debug('临时目录已清理。')
    except Exception as e:
        logger.error(f'清理临时目录失败: {e}')


def main():
    """主函数"""
    try:
        # --- 新增：架构自动检测逻辑 ---
        system_arch_raw = platform.machine().lower()
        default_arch = 'amd64'  # 默认兜底值

        # 简单的架构映射
        if system_arch_raw in ['x86_64', 'amd64']:
            default_arch = 'amd64'
        elif system_arch_raw in ['aarch64', 'arm64']:
            default_arch = 'arm64'
        elif system_arch_raw.startswith('arm'):
            default_arch = 'arm'
        # ---------------------------

        parser = argparse.ArgumentParser(description="Docker 镜像拉取工具")
        parser.add_argument("-i", "--image", required=False,
                            help="Docker 镜像名称（例如：nginx:latest 或 harbor.abc.com/abc/nginx:1.26.0）")
        parser.add_argument("-q", "--quiet", action="store_true", help="静默模式，减少交互")
        parser.add_argument("-r", "--custom_registry", help="自定义仓库地址（例如：harbor.abc.com）")

        # 修改帮助文本，显示检测到的默认架构
        parser.add_argument("-a", "--arch",
                            help=f"架构 (默认: {default_arch}, 当前设备检测为: {system_arch_raw})")

        parser.add_argument("-u", "--username", help="Docker 仓库用户名")
        parser.add_argument("-p", "--password", help="Docker 仓库密码")
        parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {VERSION}", help="显示版本信息")
        parser.add_argument("--debug", action="store_true", help="启用调试模式，打印请求 URL 和连接状态")

        # 显示程序的信息
        logger.info(f'欢迎使用 Docker 镜像拉取工具 {VERSION}')
        logger.info(f'当前系统架构: {system_arch_raw} (默认将拉取: {default_arch})')

        args = parser.parse_args()

        if args.debug:
            logger.setLevel(logging.DEBUG)

        # 获取镜像名称
        if not args.image:
            args.image = input("请输入 Docker 镜像名称（例如：nginx:latest 或 harbor.abc.com/abc/nginx:1.26.0）：").strip()
            if not args.image:
                logger.error("错误：镜像名称是必填项。")
                return

        # --- 修改：架构设置逻辑 ---
        if not args.arch:
            if args.quiet:
                # 静默模式下，直接使用自动检测的架构
                args.arch = default_arch
            else:
                # 交互模式下，提示用户（带默认值）
                user_input = input(f"请输入架构（默认: {default_arch}）：").strip()
                args.arch = user_input if user_input else default_arch
        # -----------------------

        # 获取自定义仓库地址
        if not args.custom_registry and not args.quiet:
            args.custom_registry = input("请输入自定义仓库地址: （默认 dockerhub）").strip()

        # 解析镜像信息
        registry, repository, img, tag = parse_image_input(args)

        # 获取认证信息
        if not args.username and not args.quiet:
            args.username = input("请输入镜像仓库用户名: ").strip()
        if not args.password and not args.quiet:
            args.password = input("请输入镜像仓库密码: ").strip()

        session = create_session()
        auth_head = None
        try:
            url = f'https://{registry}/v2/'
            logger.debug(f"获取认证信息 CURL 命令: curl '{url}'")
            resp = session.get(url, verify=False, timeout=30)

            # 处理部分仓库没有返回 WWW-Authenticate 头的情况
            if 'WWW-Authenticate' in resp.headers:
                auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
                reg_service = resp.headers['WWW-Authenticate'].split('"')[3]
                auth_head = get_auth_head(session, auth_url, reg_service, repository, args.username, args.password)
            else:
                # 部分私有仓库可能不需要 token 或者通过其他方式认证，尝试匿名或直接访问
                auth_head = {}

                # 获取清单
            resp, http_code = fetch_manifest(session, registry, repository, tag, auth_head)

            # 处理 401 认证请求
            if http_code == 401:
                if 'WWW-Authenticate' in resp.headers:
                    auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
                    reg_service = resp.headers['WWW-Authenticate'].split('"')[3]

                use_auth = input(f"当前仓库 {registry}，需要登录？(y/n, 默认: y): ").strip().lower() or 'y'
                if use_auth == 'y':
                    args.username = input("请输入用户名: ").strip()
                    args.password = input("请输入密码: ").strip()
                    auth_head = get_auth_head(session, auth_url, reg_service, repository, args.username, args.password)
                    resp, http_code = fetch_manifest(session, registry, repository, tag, auth_head)

        except requests.exceptions.RequestException as e:
            logger.error(f'连接仓库失败: {e}')
            raise

        resp_json = resp.json()

        # 处理多架构镜像
        manifests = resp_json.get('manifests')
        if manifests is not None:
            archs = [m.get('annotations', {}).get('com.docker.official-images.bashbrew.arch') or
                     m.get('platform', {}).get('architecture')
                     for m in manifests if m.get('platform', {}).get('os') == 'linux']

            # 打印架构列表
            if archs:
                logger.debug(f'当前可用架构：{", ".join(archs)}')

            if len(archs) == 1:
                args.arch = archs[0]
                logger.info(f'自动选择唯一可用架构: {args.arch}')

            # 如果之前选择的默认架构不在可用列表中，再次询问
            if args.arch not in archs:
                if args.quiet:
                    logger.warning(f"默认架构 {args.arch} 不在可用列表 {archs} 中，尝试使用第一个可用架构。")
                    args.arch = archs[0]
                else:
                    logger.warning(f"注意：您选择的架构 {args.arch} 不在当前镜像支持列表中。")
                    args.arch = input(f"请重新输入架构（可选: {', '.join(archs)}）：").strip() or archs[0]

            digest = select_manifest(manifests, args.arch)
            if not digest:
                logger.error(f'在清单中找不到指定的架构 {args.arch}')
                return

            # 构造请求
            url = f'https://{registry}/v2/{repository}/manifests/{digest}'
            headers = ' '.join([f"-H '{key}: {value}'" for key, value in auth_head.items()])
            curl_command = f"curl '{url}' {headers}"
            logger.debug(f'获取架构清单 CURL 命令: {curl_command}')

            # 获取清单
            manifest_resp = session.get(url, headers=auth_head, verify=False, timeout=30)
            try:
                manifest_resp.raise_for_status()
                resp_json = manifest_resp.json()
            except Exception as e:
                logger.error(f'获取架构清单失败: {e}')
                return

            if 'layers' not in resp_json:
                logger.error('错误：清单中没有层')
                return

            if 'config' not in resp_json:
                logger.error('错误：清单中没有配置信息')
                return

        # 最终检查：确保清单完整
        if 'layers' not in resp_json or 'config' not in resp_json:
            logger.error('错误：清单格式不完整，缺少必要字段')
            logger.debug(f'清单内容: {resp_json.keys()}')
            return

        logger.info(f'仓库地址：{registry}')
        logger.info(f'镜像：{repository}')
        logger.info(f'标签：{tag}')
        logger.info(f'架构：{args.arch}')

        # 下载镜像层
        imgdir = 'tmp'
        os.makedirs(imgdir, exist_ok=True)
        logger.info('开始下载')

        # 根据镜像类型，提供正确的imgparts
        if registry == 'registry-1.docker.io' and repository.startswith('library/'):
            # Docker Hub
            imgparts = []  # 官方镜像不需要前缀
        else:
            #
            imgparts = repository.split('/')[:-1]

        download_layers(session, registry, repository, resp_json['layers'], auth_head, imgdir, resp_json, imgparts, img,
                        tag, args.arch)

        # 打包镜像
        output_file = create_image_tar(imgdir, repository, tag, args.arch)
        logger.info(f'镜像已保存为: {output_file}')
        logger.info(f'可使用以下命令导入镜像: docker load -i {output_file}')
        if registry not in ("registry-1.docker.io", "docker.io"):
            logger.info(f'您可能需要: docker tag {repository}:{tag} {registry}/{repository}:{tag}')

    except KeyboardInterrupt:
        logger.info('用户取消操作。')
    except requests.exceptions.RequestException as e:
        logger.error(f'网络连接失败: {e}')
    except json.JSONDecodeError as e:
        logger.error(f'JSON解析失败: {e}')
    except FileNotFoundError as e:
        logger.error(f'文件操作失败: {e}')
    except argparse.ArgumentError as e:
        logger.error(f'命令行参数错误: {e}')
    except Exception as e:
        logger.error(f'程序运行过程中发生异常: {e}')
        import traceback
        logger.debug(traceback.format_exc())

    finally:
        cleanup_tmp_dir()
        try:
            # 只有在非 quiet 模式下才暂停，方便自动化脚本调用
            if 'args' in locals() and not args.quiet:
                input("按任意键退出程序...")
        except (KeyboardInterrupt, EOFError):
            pass
        sys.exit(0)


if __name__ == '__main__':
    main()
