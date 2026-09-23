import os
import queue
import re
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread
from typing import List, Tuple, Dict, Any, Optional, Set

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.scraping import ScrapingChain, ScrapingConfig
from app.sdk.config import settings
from app.sdk.media import MediaInfo, MetaInfoPath
from app.db.oper.transferhistory import TransferHistoryOper
from app.sdk.media import NfoReader
from app.sdk.logging import logger
from app.plugins import _PluginBase
from app.schemas import MediaSource, MediaType
from app.schemas.types import ScrapingMetadata, ScrapingTarget
from app.sdk.media import resolve_media_identity
from app.sdk.utilities import SystemUtils

# 刮削目标：（目标路径，媒体类型，目标形态，媒体来源，媒体ID）
ScrapeTarget = Tuple[Path, MediaType, str, Optional[MediaSource], Optional[str]]


class LibraryScraperEchoCC(_PluginBase):
    # 插件名称
    plugin_name = "媒体库刮削专享版"
    # 插件描述
    plugin_desc = "定时对媒体库进行刮削，按系统刮削开关增量补齐缺失元数据和图片；发现阶段剪枝并边扫边刮加速大库扫描，支持手动终止。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "3.3.0"
    # 插件作者
    plugin_author = "EchoCC"
    # 作者主页
    author_url = "https://github.com/EchoCC"
    # 插件配置项ID前缀
    plugin_config_prefix = "libraryscraperechocc_"
    # 加载顺序
    plugin_order = 7
    # 可使用的用户级别
    user_level = 1

    # 覆盖模式取值：增量刮削并补充元数据 / 覆盖所有元数据和图片
    _mode_incremental = "incremental"
    _mode_force_all = "force_all"

    # 私有属性
    _scheduler = None
    _scraper = None
    # 限速开关
    _enabled = False
    _onlyonce = False
    _cron = None
    _mode = _mode_incremental
    _scraper_paths = ""
    _exclude_paths = ""
    # 退出事件
    _event = Event()
    # 运行位与消费线程引用：手动终止接口跨线程读写，须在锁内变更
    _stop_lock = Lock()
    _running = False
    _consumer = None
    # 手动终止等待消费线程销毁的秒数，超时由本轮 finally 兜底回收
    _stop_join_timeout = 10
    # 刮削目标类型
    _target_dir = "dir"
    _target_file = "file"
    # 目标队列上限：队列满时反压扫描，内存不随媒体库规模线性增长
    _queue_maxsize = 64
    # 图片项对应的本地文件名（含别名），与系统刮削开关的元数据项对齐
    _image_stems = {
        ScrapingMetadata.POSTER: ("poster",),
        ScrapingMetadata.BACKDROP: ("backdrop", "fanart", "background"),
        ScrapingMetadata.LOGO: ("logo",),
        ScrapingMetadata.BANNER: ("banner",),
        ScrapingMetadata.THUMB: ("thumb",),
        ScrapingMetadata.DISC: ("disc", "cdart"),
        ScrapingMetadata.CLEARART: ("clearart",),
        ScrapingMetadata.LANDSCAPE: ("landscape",),
    }
    # 本地图片常见扩展名
    _image_extensions = (".jpg", ".jpeg", ".png", ".webp")

    def init_plugin(self, config: dict = None):

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            # 兼容旧配置的空值（不覆盖已有元数据），统一归一为增量刮削
            mode = config.get("mode") or ""
            self._mode = self._mode_force_all if mode == self._mode_force_all else self._mode_incremental
            self._scraper_paths = config.get("scraper_paths") or ""
            self._exclude_paths = config.get("exclude_paths") or ""

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self._enabled or self._onlyonce:

            if self._onlyonce:
                logger.info(f"媒体库刮削服务，立即运行一次")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(func=self.__libraryscraperechocc, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="媒体库刮削")
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "enabled": self._enabled,
                    "cron": self._cron,
                    "mode": self._mode,
                    "scraper_paths": self._scraper_paths,
                    "exclude_paths": self._exclude_paths
                })
                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """注册手动终止接口。"""
        return [
            {
                "path": "/stop",
                "endpoint": self.api_stop,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "手动终止刮削并销毁消费线程",
            }
        ]

    def api_stop(self) -> Dict[str, Any]:
        """
        手动终止接口：置位停止事件让扫描与入队立即停下，并等待消费线程退出
        销毁；超时则由运行任务自身的 finally 兜底回收，不影响下次触发。
        必须返回 {success, message, data} 三键信封：前端按“恰好三键”校验，
        缺 data 会被当成插件自由响应原样交付，请求层不会弹出组件提示。
        """
        with self._stop_lock:
            if not self._running:
                return {"success": False, "message": "当前没有正在运行的刮削任务", "data": None}
            consumer = self._consumer
            self._event.set()
        logger.info(f"手动终止：已置位停止事件，扫描与消费开始停止 ...")
        if not consumer:
            # 消费线程尚未登记（极小窗口），事件已置位，本轮会自行结束
            return {"success": True, "message": "已发出终止信号，任务即将停止", "data": None}
        consumer.join(timeout=self._stop_join_timeout)
        if consumer.is_alive():
            return {"success": True, "message": "已停止扫描，消费线程将在当前目标完成后销毁", "data": None}
        return {"success": True, "message": "已停止运行，消费线程已销毁", "data": None}

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [{
                "id": "LibraryScraperEchoCC",
                "name": "媒体库刮削",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__libraryscraperechocc,
                "kwargs": {}
            }]
        elif self._enabled:
            return [{
                "id": "LibraryScraperEchoCC",
                "name": "媒体库刮削",
                "trigger": CronTrigger.from_crontab("0 0 */7 * *"),
                "func": self.__libraryscraperechocc,
                "kwargs": {}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 4
                                },
                                'content': [
                                    {
                                        'component': 'VBtn',
                                        'props': {
                                            'color': 'error',
                                            'variant': 'tonal',
                                            'block': True,
                                            'prepend-icon': 'mdi-stop-circle',
                                            # feedback=all 让请求层展示成功消息；失败信封（如无任务）
                                            # 默认弹错误提示，全程由组件提示替代阻塞式 alert
                                            'onclick': "function(e) { window.MoviePilotAPI.post('plugin/LibraryScraperEchoCC/stop', {}, { feedback: 'all' }).catch(function(err) { console.error(err) }) }",
                                        },
                                        'text': '手动终止',
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '覆盖模式',
                                            'items': [
                                                {'title': '增量刮削并补充元数据', 'value': 'incremental'},
                                                {'title': '覆盖所有元数据和图片', 'value': 'force_all'},
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'scraper_paths',
                                            'label': '削刮路径',
                                            'rows': 5,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_paths',
                                            'label': '排除路径',
                                            'rows': 2,
                                            'placeholder': '每一行一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '刮削路径后拼接#电视剧/电影，强制指定该媒体路径媒体类型。'
                                                    '不加默认根据文件名自动识别媒体类型。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '增量刮削并补充元数据：按 高级设置-媒体-刮削开关 逐项检查本地NFO和图片，'
                                                    '仅在产物缺失或NFO缺少简介(plot)时才执行刮削，全部齐全的目标直接跳过。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 0 */7 * *",
            "mode": "incremental",
            "scraper_paths": "",
            "err_hosts": ""
        }

    def get_page(self) -> List[dict]:
        pass

    def __libraryscraperechocc(self):
        """
        开始刮削媒体库

        扫描为生产者、刮削为消费者：目标边发现边入有界队列，消费线程并行
        刮削，无需等待全量扫描结束；队列满时反压扫描，限制大库内存占用。
        运行位与停止事件归本轮所有，手动终止后在此复位以便再次触发。
        """
        if not self._scraper_paths:
            return
        # 增量模式按系统刮削开关判断本地产物是否完整；覆盖模式无需判定
        scraping_config = self.__scraping_config()
        # 运行位预留：拒绝并发触发，避免运行交错时停止事件被误复位
        with self._stop_lock:
            if self._running:
                logger.warning(f"已有刮削任务在运行，本次触发已忽略")
                return
            self._running = True
        started = False
        try:
            # 有界目标队列：满时反压扫描，内存不随媒体库规模增长
            target_queue: queue.Queue = queue.Queue(maxsize=self._queue_maxsize)
            # 集合去重：同一目标只入队一次
            seen_targets: Set[Tuple[Path, MediaType, str]] = set()
            # 消费线程先于扫描启动，边扫描边刮削
            consumer = Thread(
                target=self.__consume_targets,
                args=(target_queue, scraping_config),
                daemon=True,
                name="libraryscraperechocc-consumer",
            )
            consumer.start()
            started = True
            with self._stop_lock:
                self._consumer = consumer
            self.__scan_and_enqueue(
                paths=self._scraper_paths.split("\n"),
                exclude_paths=self._exclude_paths.split("\n"),
                target_queue=target_queue,
                seen_targets=seen_targets,
                consumer=consumer,
            )
        finally:
            if started:
                # 结束信号交还队列并回收消费线程；停止或扫描异常路径同样保证
                self.__enqueue_target(target_queue, None, consumer)
                consumer.join()
            with self._stop_lock:
                self._running = False
                self._consumer = None
                # 复位停止事件：手动终止后任务仍可再次触发
                self._event.clear()

    def __scan_and_enqueue(
            self,
            paths: List[str],
            exclude_paths: List[str],
            target_queue: queue.Queue,
            seen_targets: Set[Tuple[Path, MediaType, str]],
            consumer: Thread,
    ):
        """
        扫描配置的媒体根目录，把去重后的刮削目标边发现边写入队列。
        """
        for path in paths:
            if not path:
                continue
            # 强制指定该路径媒体类型
            mtype = None
            if str(path).count("#") == 1:
                mtype = next(
                    (mediaType for mediaType in MediaType.__members__.values() if
                     mediaType.value == str(str(path).split("#")[1])),
                    None)
                path = str(path).split("#")[0]
            # 判断路径是否存在
            scraper_path = Path(path)
            if not scraper_path.exists():
                logger.warning(f"媒体库刮削路径不存在：{path}")
                continue
            logger.info(f"开始检索目录：{path} {mtype} ...")
            # 遍历所有文件：排除目录在遍历期剪枝，不再全量枚举后逐文件过滤
            files = self.__list_scan_files(
                scraper_path=scraper_path,
                extensions=settings.RMT_MEDIAEXT,
                exclude_paths=exclude_paths,
            )
            for file_path in files:
                if self._event.is_set():
                    logger.info(f"媒体库刮削服务停止")
                    return
                if mtype and not self.__match_forced_type_path(
                        file_path=file_path,
                        scraper_path=scraper_path,
                        mtype=mtype
                ):
                    logger.debug(f"{file_path} 不属于强制指定的{mtype.value}目录，跳过 ...")
                    continue
                # 识别是电影还是电视剧，强制类型只作为默认值，不污染后续文件识别结果
                file_meta = MetaInfoPath(file_path)
                file_mtype = mtype
                if not file_mtype:
                    file_mtype = file_meta.type
                    if file_mtype == MediaType.UNKNOWN:
                        file_mtype = self.__infer_type_from_path(file_path=file_path, scraper_path=scraper_path)
                scraper_item = self.__get_scrape_item(
                    file_path=file_path,
                    scraper_path=scraper_path,
                    mtype=file_mtype,
                    media_source=file_meta.media_source,
                    media_id=file_meta.media_id,
                )
                if not scraper_item:
                    continue
                target_key = scraper_item[:3]
                if target_key in seen_targets:
                    continue
                seen_targets.add(target_key)
                logger.info(f"发现刮削目标：{scraper_item}")
                if not self.__enqueue_target(target_queue, scraper_item, consumer):
                    if self._event.is_set():
                        logger.info(f"媒体库刮削服务停止")
                    else:
                        logger.error(f"消费线程异常退出，停止扫描：{scraper_item}")
                    return

    def __consume_targets(
            self,
            target_queue: queue.Queue,
            scraping_config: Optional[ScrapingConfig],
    ):
        """
        消费目标队列并执行刮削。

        超时轮询等待结束信号，同时响应停止事件；单个目标失败仅记录日志，
        不中断管道。增量判定与汇总日志由消费端负责。
        """
        skipped = 0
        consumed = 0
        while True:
            if self._event.is_set():
                logger.info(f"媒体库刮削服务停止")
                break
            try:
                item = target_queue.get(timeout=0.5)
            except queue.Empty:
                # 队列暂空：等待生产者继续产出或写入结束信号
                continue
            if item is None:
                break
            consumed += 1
            try:
                # 目标目录枚举媒体文件：供增量判定和 NFO 写盘约定复用
                media_files = (SystemUtils.list_files(item[0], settings.RMT_MEDIAEXT)
                               if item[2] == self._target_dir else [item[0]])
                if scraping_config and not self.__has_missing(
                        scraping_config, item[0], item[1], item[2], media_files):
                    skipped += 1
                    logger.debug(f"{item[0]} 增量刮削：元数据齐全，跳过")
                    continue
                logger.info(f"开始刮削目标：{item[0]} ...")
                self.__scrape_path(
                    path=item[0],
                    mtype=item[1],
                    target_type=item[2],
                    media_source=item[3],
                    media_id=item[4],
                    media_files=media_files,
                )
            except Exception as err:
                # 单点失败不拖垮管道：大库扫描耗时长，中断代价高
                logger.error(f"刮削目标失败：{item[0]} - {str(err)}")
        if consumed == 0 and not self._event.is_set():
            logger.info(f"未发现需要刮削的目录")
        elif skipped:
            logger.info(f"增量刮削：{skipped} 个目标元数据齐全，已跳过")

    def __enqueue_target(
            self,
            target_queue: queue.Queue,
            item: Optional[ScrapeTarget],
            consumer: Thread,
    ) -> bool:
        """
        入队刮削目标或结束信号；队列满时反压等待消费者腾位。

        停止事件置位或消费线程意外退出时放弃入队，避免生产者死锁。
        返回是否成功入队。
        """
        while not self._event.is_set() and consumer.is_alive():
            try:
                target_queue.put(item, timeout=0.5)
                return True
            except queue.Full:
                continue
        return False

    @staticmethod
    def __get_scrape_item(
            file_path: Path,
            scraper_path: Path,
            mtype: MediaType,
            media_source: Optional[MediaSource] = None,
            media_id: Optional[str] = None,
    ) -> Optional[ScrapeTarget]:
        """
        根据扫描根目录和重命名格式，计算真正需要刮削的媒体目录。
        分类目录通常位于扫描根目录下方，必须用相对路径计算，否则会被误当成媒体目录。
        """
        if not file_path or not scraper_path or not mtype:
            return None

        rename_format = settings.TV_RENAME_FORMAT if mtype == MediaType.TV else settings.MOVIE_RENAME_FORMAT
        rename_format_level = len(rename_format.strip("/").split("/")) - 1
        try:
            relative_path = file_path.relative_to(scraper_path)
        except ValueError:
            relative_path = Path(file_path.name)

        if rename_format_level >= 1:
            relative_parts = Path(relative_path).parts
            # 重命名格式中包含几层目录，就从文件往上取几层目录；前缀分类目录不会参与计算。
            if len(relative_parts) > rename_format_level:
                media_path = scraper_path.joinpath(*relative_parts[:-rename_format_level])
                return media_path, mtype, LibraryScraperEchoCC._target_dir, media_source, media_id

        # 扁平目录或自定义重命名格式无目录层级时，退回到单文件刮削，避免分类目录识别失败。
        return file_path, mtype, LibraryScraperEchoCC._target_file, media_source, media_id

    @staticmethod
    def __list_scan_files(
            scraper_path: Path,
            extensions: List[str],
            exclude_paths: List[str],
    ) -> List[Path]:
        """
        遍历扫描根目录收集媒体文件，进入子目录前剪枝排除目录。

        与全量枚举后逐文件过滤相比，遍历期剪枝不再下钻排除目录，
        大媒体库的扫描开销随排除范围直接下降。文件级排除在收集时同步
        过滤，覆盖排除单个文件以及扫描根本身位于排除目录的场景。
        匹配语义与 SystemUtils.list_files 保持一致（后缀正则、不跟随软链接文件）。

        :param scraper_path: 扫描根目录
        :param extensions: 媒体扩展名列表，例如 ['.mkv', '.mp4']
        :param exclude_paths: 排除路径原始配置项（每行一个，可为空项）
        :return: 未被排除的媒体文件列表
        """
        excludes = [Path(exclude) for exclude in exclude_paths if exclude]
        pattern = (
            re.compile(r".*(" + "|".join(extensions) + r")$", re.IGNORECASE)
            if extensions else re.compile(r".*")
        )
        media_files: List[Path] = []

        def _is_excluded(target: Path) -> bool:
            return any(target.is_relative_to(exclude) for exclude in excludes)

        def _scan_directory(dir_path: Path) -> None:
            try:
                with os.scandir(dir_path) as entries:
                    for entry in entries:
                        try:
                            entry_path = Path(entry.path)
                            if entry.is_file(follow_symlinks=False):
                                if pattern.match(entry.name) and not _is_excluded(entry_path):
                                    media_files.append(entry_path)
                            elif entry.is_dir() and _is_excluded(entry_path):
                                # 遍历期剪枝：排除目录不再下钻
                                logger.debug(f"{entry_path} 在排除目录中，剪枝跳过 ...")
                            elif entry.is_dir():
                                _scan_directory(entry_path)
                        except (OSError, PermissionError):
                            continue
            except (OSError, PermissionError):
                pass

        # 单文件扫描根与 SystemUtils.list_files 行为对齐，仍需过排除判定
        if scraper_path.is_file():
            return [] if _is_excluded(scraper_path) else [scraper_path]
        if not scraper_path.is_dir():
            return []
        _scan_directory(scraper_path)
        return media_files

    @staticmethod
    def __match_forced_type_path(file_path: Path, scraper_path: Path, mtype: MediaType) -> bool:
        """
        强制指定媒体类型时，如果扫描根目录下同时存在“电影/电视剧”分类，则只处理匹配类型的目录。
        """
        if mtype not in (MediaType.MOVIE, MediaType.TV):
            return True
        try:
            relative_parts = file_path.relative_to(scraper_path).parts
        except ValueError:
            return True
        media_type_parts = {MediaType.MOVIE.value, MediaType.TV.value}.intersection(relative_parts)
        return not media_type_parts or mtype.value in media_type_parts

    @staticmethod
    def __infer_type_from_path(file_path: Path, scraper_path: Path) -> MediaType:
        """
        文件名无法识别类型时，从扫描根目录下的“电影/电视剧”分类层推断媒体类型。
        """
        try:
            relative_parts = file_path.relative_to(scraper_path).parts
        except ValueError:
            relative_parts = file_path.parts
        if MediaType.TV.value in relative_parts:
            return MediaType.TV
        if MediaType.MOVIE.value in relative_parts:
            return MediaType.MOVIE
        return MediaType.UNKNOWN

    def __scrape_path(
            self,
            path: Path,
            mtype: MediaType,
            target_type: str = _target_dir,
            media_source: Optional[MediaSource] = None,
            media_id: Optional[str] = None,
            media_files: Optional[List[Path]] = None,
    ):
        """
        刮削一个媒体目录或媒体文件

        :param media_files: 目标下已枚举的媒体文件，缺省时按目标类型自行枚举
        """
        media_source, media_id = resolve_media_identity(
            media_source=media_source,
            media_id=media_id,
        )
        if media_files is None:
            media_files = (
                SystemUtils.list_files(path, settings.RMT_MEDIAEXT)
                if target_type == self._target_dir else [path]
            )
        # 优先读取本地 NFO 文件；NFO 无合法身份时保留文件路径中的统一身份。
        nfo_reads = [
            nfo_path
            for nfo_path, _, _ in self.__nfo_expectations(path, mtype, target_type, media_files)
        ]
        if target_type == self._target_dir and mtype == MediaType.MOVIE:
            # 链路对普通电影目录不写目录级 NFO，这里仅兼容外部工具写入的命名
            nfo_reads.extend((path / f"{path.name}.nfo", path / "movie.nfo"))
        for nfo_path in dict.fromkeys(nfo_reads):
            if not nfo_path.exists():
                continue
            nfo_source, nfo_media_id = self.__get_media_identity_from_nfo(nfo_path)
            if nfo_source:
                media_source, media_id = nfo_source, nfo_media_id
                break
        if media_source and media_id:
            logger.info(f"读取到本地 NFO 媒体身份：{media_source.value}:{media_id}")
            mediainfo = self.chain.recognize_media(
                media_source=media_source,
                media_id=str(media_id),
                mtype=mtype,
            )
        else:
            # 按名称识别
            meta = MetaInfoPath(path)
            meta.type = mtype
            mediainfo = self.chain.recognize_media(meta=meta)
        if not mediainfo:
            if target_type == self._target_dir:
                # 目录名无法识别时，通常是分类目录，继续尝试其中的具体媒体文件。
                self.__scrape_child_files(path=path, mtype=mtype)
                return
            logger.warn(f"未识别到媒体信息：{path}")
            return

        # 不跟随远端标题时，按统一媒体身份找回整理历史中的标题。
        if not settings.SCRAP_FOLLOW_TMDB:
            transfer_history = TransferHistoryOper().get_by_media_identity(
                media_source=mediainfo.media_source,
                media_id=mediainfo.media_id,
                mtype=mediainfo.type.value,
            )
            if transfer_history:
                mediainfo.title = transfer_history.title
        # 增量模式先记录已存在但缺简介的 NFO：刮削不会重写它们，需在刮削后单独补充
        pending_nfo = self.__collect_pending_nfo(
            path=path, mtype=mtype, target_type=target_type, media_files=media_files)
        # 获取图片
        self.chain.obtain_images(mediainfo)
        # 刮削
        item_path = str(path).replace("\\", "/")
        if target_type == self._target_dir:
            item_path = f"{item_path}/"
        ScrapingChain().scrape_metadata(
            fileitem=schemas.FileItem(
                storage="local",
                type=target_type,
                path=item_path,
                name=path.name,
                basename=path.stem,
                extension=path.suffix[1:] if target_type == self._target_file else None,
                modify_time=path.stat().st_mtime,
            ),
            mediainfo=mediainfo,
            overwrite=self._mode == self._mode_force_all
        )
        # 补充增量模式收集的缺失简介
        self.__fill_missing_plot(pending_nfo, mediainfo)
        logger.info(f"{path} 刮削完成")

    def __scrape_child_files(self, path: Path, mtype: MediaType):
        """
        分类目录无法作为单个媒体识别时，继续按目录内的媒体文件逐个刮削。
        """
        child_files = SystemUtils.list_files(path, settings.RMT_MEDIAEXT)
        if not child_files:
            logger.warn(f"未识别到媒体信息：{path}")
            return
        logger.info(f"{path} 可能是分类目录，开始刮削目录内媒体文件 ...")
        scraping_config = self.__scraping_config()
        for child_file in child_files:
            if self._event.is_set():
                logger.info(f"媒体库刮削服务停止")
                return
            child_mtype = mtype
            child_meta = MetaInfoPath(child_file)
            if not child_mtype:
                child_mtype = child_meta.type
            if scraping_config and not self.__has_missing(
                    scraping_config, child_file, child_mtype, self._target_file, [child_file]):
                logger.debug(f"{child_file} 增量刮削：元数据齐全，跳过")
                continue
            self.__scrape_path(
                path=child_file,
                mtype=child_mtype,
                target_type=self._target_file,
                media_source=child_meta.media_source,
                media_id=child_meta.media_id,
                media_files=[child_file],
            )

    @staticmethod
    def __get_media_identity_from_nfo(file_path: Path) -> Tuple[Optional[MediaSource], Optional[str]]:
        """
        从 NFO 中读取第一个可识别的固定来源媒体身份。

        :param file_path: NFO 文件路径
        :return: 媒体来源枚举与数据源原生 ID
        """
        if not file_path:
            return None, None
        source_xpaths = {
            MediaSource.TMDB: (
                "uniqueid[@type='Tmdb']",
                "uniqueid[@type='tmdb']",
                "uniqueid[@type='TMDB']",
                "tmdbid",
            ),
            MediaSource.IMDb: (
                "uniqueid[@type='Imdb']",
                "uniqueid[@type='imdb']",
                "uniqueid[@type='IMDB']",
                "imdbid",
            ),
            MediaSource.TVDB: (
                "uniqueid[@type='Tvdb']",
                "uniqueid[@type='tvdb']",
                "uniqueid[@type='TVDB']",
                "tvdbid",
            ),
        }
        try:
            reader = NfoReader(file_path)
            for source, xpaths in source_xpaths.items():
                for xpath in xpaths:
                    media_source, media_id = resolve_media_identity(
                        media_source=source,
                        media_id=reader.get_element_value(xpath),
                    )
                    if media_source:
                        return media_source, media_id
        except Exception as err:
            logger.warn(f"从 NFO 文件中获取媒体身份失败：{str(err)}")
        return None, None

    def __scraping_config(self) -> Optional[ScrapingConfig]:
        """增量模式返回系统刮削开关配置；覆盖模式无需判定。"""
        if self._mode == self._mode_force_all:
            return None
        return ScrapingConfig.from_system_config()

    @staticmethod
    def __scraping_target(mtype: MediaType) -> Optional[ScrapingTarget]:
        """媒体类型映射到系统刮削开关的目标类型；未知类型不参与增量判定。"""
        if mtype == MediaType.MOVIE:
            return ScrapingTarget.MOVIE
        if mtype == MediaType.TV:
            return ScrapingTarget.TV
        return None

    @staticmethod
    def __direct_media_files(path: Path, media_files: List[Path]) -> List[Path]:
        """筛出目标目录直属的媒体文件；链路对电影目录只处理直属文件（子目录跳过）。"""
        return [media_file for media_file in media_files if media_file.parent == path]

    @staticmethod
    def __is_bluray_dir(path: Path) -> bool:
        """与链路 contains_bluray_subdirectories 一致：存在 BDMV/CERTIFICATE 子目录即蓝光原盘。"""
        return any((path / name).is_dir() for name in ("BDMV", "CERTIFICATE"))

    @staticmethod
    def __nfo_expectations(
            path: Path,
            mtype: MediaType,
            target_type: str,
            media_files: List[Path],
    ) -> List[Tuple[Path, Path, bool]]:
        """
        按链路写盘约定列出目标根级 NFO 的期望产物。

        文件目标写 {文件主名}.nfo（与文件同级）；普通电影目录由链路递归刮削
        直属媒体文件，逐个写 {媒体文件主名}.nfo，蓝光原盘目录跳过子文件只写
        目录名 NFO；电视剧根目录写 tvshow.nfo。
        返回 (NFO 路径, 元数据解析路径, 是否按剧集生成) 三元组。

        :param path: 媒体目录或媒体文件路径
        :param mtype: 媒体类型
        :param target_type: 刮削目标类型（目录/文件）
        :param media_files: 目标下的媒体文件（目录目标为全部媒体文件，文件目标为自身）
        :return: 期望 NFO 三元组列表
        """
        if target_type == LibraryScraperEchoCC._target_file:
            # 文件目标：NFO 与文件同名同级；剧集文件按季集语义生成
            return [(path.with_suffix(".nfo"), path, mtype == MediaType.TV)]
        if mtype == MediaType.MOVIE:
            if LibraryScraperEchoCC.__is_bluray_dir(path):
                # 蓝光原盘目录：链路跳过子文件，只写目录名 NFO
                return [(path / f"{path.name}.nfo", path, False)]
            # 普通电影目录：链路只处理直属媒体文件，逐个写同名 NFO（非目录名命名）
            return [
                (media_file.with_suffix(".nfo"), media_file, False)
                for media_file in LibraryScraperEchoCC.__direct_media_files(path, media_files)
            ]
        return [(path / "tvshow.nfo", path, False)]

    @staticmethod
    def __nfo_has_plot(nfo_path: Path) -> bool:
        """NFO 中 plot 元素非空才算简介完整；读取失败视为缺失以便重建。"""
        try:
            plot = NfoReader(nfo_path).get_element_value("plot")
        except Exception as err:
            logger.debug(f"读取 NFO 简介失败：{nfo_path} - {str(err)}")
            return False
        return bool(plot and str(plot).strip())

    @staticmethod
    def __missing_nfo(
            scraping_config: ScrapingConfig,
            item: ScrapingTarget,
            expectations: List[Tuple[Path, Path, bool]],
    ) -> bool:
        """按刮削开关判断根级 NFO 是否缺失；跳过项不判定，覆盖策略视为缺失。"""
        option = scraping_config.option(item, ScrapingMetadata.NFO)
        if option.is_skip:
            return False
        if option.is_overwrite:
            return True
        # 链路可能写多个 NFO（电影目录逐文件），任一缺失或缺简介即需刮削
        return any(
            not nfo_path.exists() or not LibraryScraperEchoCC.__nfo_has_plot(nfo_path)
            for nfo_path, _meta_path, _is_episode in expectations
        )

    @staticmethod
    def __missing_images(
            scraping_config: ScrapingConfig,
            item: ScrapingTarget,
            directory: Path,
    ) -> bool:
        """按刮削开关逐项判断目录内图片是否缺失，任一项缺失即需刮削。"""
        for metadata, stems in LibraryScraperEchoCC._image_stems.items():
            option = scraping_config.option(item, metadata)
            if option.is_skip:
                continue
            if option.is_overwrite:
                return True
            exists = any(
                (directory / f"{stem}{ext}").exists()
                for stem in stems
                for ext in LibraryScraperEchoCC._image_extensions
            )
            if not exists:
                return True
        return False

    @staticmethod
    def __missing_episode(scraping_config: ScrapingConfig, media_files: List[Path]) -> bool:
        """按刮削开关判断集级同名 NFO（含简介）与缩略图是否缺失。"""
        nfo_option = scraping_config.option(ScrapingTarget.EPISODE, ScrapingMetadata.NFO)
        thumb_option = scraping_config.option(ScrapingTarget.EPISODE, ScrapingMetadata.THUMB)
        for media_file in media_files:
            if not nfo_option.is_skip:
                if nfo_option.is_overwrite:
                    return True
                nfo = media_file.with_suffix(".nfo")
                if not nfo.exists() or not LibraryScraperEchoCC.__nfo_has_plot(nfo):
                    return True
            if not thumb_option.is_skip:
                if thumb_option.is_overwrite:
                    return True
                if not any(
                        media_file.with_suffix(ext).exists()
                        for ext in LibraryScraperEchoCC._image_extensions
                ):
                    return True
        return False

    @staticmethod
    def __has_missing(
            scraping_config: ScrapingConfig,
            path: Path,
            mtype: MediaType,
            target_type: str,
            media_files: List[Path],
    ) -> bool:
        """
        增量模式下判断目标是否存在需刮削的缺失产物。

        按系统刮削开关逐项检查本地 NFO 和图片；开关为跳过的项不参与判定，
        NFO 已存在但简介为空同样视为缺失；未知媒体类型保守继续刮削。

        :param scraping_config: 系统刮削开关配置
        :param path: 媒体目录或媒体文件路径
        :param mtype: 媒体类型
        :param target_type: 刮削目标类型（目录/文件）
        :param media_files: 目标下的媒体文件（目录目标为全部媒体文件，文件目标为自身）
        :return: 是否存在缺失
        """
        item = LibraryScraperEchoCC.__scraping_target(mtype)
        if item is None:
            return True
        if target_type == LibraryScraperEchoCC._target_file and item == ScrapingTarget.TV:
            # 剧集文件只产生集级 NFO 和缩略图
            return LibraryScraperEchoCC.__missing_episode(scraping_config, media_files)
        # 根级 NFO：按链路写盘约定推导期望产物（电影目录为每个直属媒体文件一个 NFO）
        expectations = LibraryScraperEchoCC.__nfo_expectations(path, mtype, target_type, media_files)
        if LibraryScraperEchoCC.__missing_nfo(scraping_config, item, expectations):
            return True
        # 根级图片：文件目标的图片写入同级目录，目录目标写入目录内
        root = path if target_type == LibraryScraperEchoCC._target_dir else path.parent
        if LibraryScraperEchoCC.__missing_images(scraping_config, item, root):
            return True
        # 电视剧目录再判目录内的集级产物
        if target_type == LibraryScraperEchoCC._target_dir and item == ScrapingTarget.TV:
            return LibraryScraperEchoCC.__missing_episode(scraping_config, media_files)
        return False

    def __collect_pending_nfo(
            self,
            path: Path,
            mtype: MediaType,
            target_type: str,
            media_files: List[Path],
    ) -> List[Tuple[Path, Path, bool]]:
        """
        收集已存在但缺少简介的 NFO，供刮削后单独补充。

        增量模式不覆盖已有 NFO，刮削流程也不会重写它，只能先记录、
        待刮削完成后按元数据重新生成。返回 (NFO 路径, 元数据解析路径, 是否按剧集生成)。

        :param path: 媒体目录或媒体文件路径
        :param mtype: 媒体类型
        :param target_type: 刮削目标类型（目录/文件）
        :param media_files: 目标下已枚举的媒体文件
        :return: 待补充简介的 NFO 三元组列表
        """
        scraping_config = self.__scraping_config()
        item = self.__scraping_target(mtype)
        if scraping_config is None or item is None:
            return []
        pending_nfo: List[Tuple[Path, Path, bool]] = []
        # 根级 NFO：逐个检查链路会写入的产物（电影目录为每个直属媒体文件一个 NFO）
        root_option = scraping_config.option(item, ScrapingMetadata.NFO)
        if not root_option.is_skip and not root_option.is_overwrite:
            for nfo_path, meta_path, is_episode in self.__nfo_expectations(
                    path, mtype, target_type, media_files):
                if nfo_path.exists() and not self.__nfo_has_plot(nfo_path):
                    pending_nfo.append((nfo_path, meta_path, is_episode))
        # 电视剧目录的集级 NFO
        if target_type == self._target_dir and item == ScrapingTarget.TV:
            episode_option = scraping_config.option(ScrapingTarget.EPISODE, ScrapingMetadata.NFO)
            if not episode_option.is_skip and not episode_option.is_overwrite:
                for media_file in media_files:
                    nfo = media_file.with_suffix(".nfo")
                    if nfo.exists() and not self.__nfo_has_plot(nfo):
                        pending_nfo.append((nfo, media_file, True))
        return pending_nfo

    @staticmethod
    def __fill_missing_plot(
            pending_nfo: List[Tuple[Path, Path, bool]],
            mediainfo: MediaInfo,
    ) -> None:
        """重新生成并写入收集到的缺简介 NFO；剧集无法解析季集信息时跳过。"""
        if not pending_nfo:
            return
        scraping_chain = ScrapingChain()
        for nfo_path, meta_path, is_episode in pending_nfo:
            meta = MetaInfoPath(meta_path)
            season = episode = None
            if is_episode:
                season, episode = meta.begin_season, meta.begin_episode
                if season is None or episode is None:
                    logger.debug(f"{nfo_path.name} 无法解析季集信息，跳过补充简介")
                    continue
            content = scraping_chain.metadata_nfo(
                meta=meta,
                mediainfo=mediainfo,
                season=season,
                episode=episode,
            )
            if not content:
                logger.warn(f"{nfo_path.name} NFO 内容生成失败，跳过补充简介")
                continue
            try:
                if isinstance(content, bytes):
                    nfo_path.write_bytes(content)
                else:
                    nfo_path.write_text(content, encoding="utf-8")
                logger.info(f"已为 {nfo_path.name} 补充简介")
            except OSError as err:
                logger.warn(f"补充简介失败：{nfo_path} - {str(err)}")

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))
