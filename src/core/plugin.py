"""
Plugin Framework - 插件框架

为指标、策略、数据源等提供统一的插件化加载/卸载机制。
"""

from __future__ import annotations

import importlib
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, TypeVar

import yaml
from loguru import logger

T = TypeVar("T", bound="PluginBase")


class PluginBase(ABC):
    """插件基类 - 所有可插拔组件的基础接口"""

    @abstractmethod
    def name(self) -> str:
        """插件名称"""
        ...

    @abstractmethod
    def version(self) -> str:
        """插件版本"""
        ...

    def description(self) -> str:
        """插件描述"""
        return ""

    def on_load(self) -> None:
        """加载时回调"""
        pass

    def on_unload(self) -> None:
        """卸载时回调"""
        pass


class PluginManager:
    """
    插件管理器

    通过 YAML 配置或手动注册来管理插件生命周期。

    Usage:
        manager = PluginManager()
        manager.register(MyIndicatorPlugin())
        plugin = manager.get("my_indicator")
    """

    def __init__(self) -> None:
        self._plugins: dict[str, PluginBase] = {}
        self._plugin_classes: dict[str, type[PluginBase]] = {}

    def register(self, plugin: PluginBase) -> None:
        """注册插件实例"""
        name = plugin.name()
        if name in self._plugins:
            logger.warning(f"Plugin '{name}' already registered, overwriting")
        self._plugins[name] = plugin
        plugin.on_load()
        logger.info(f"Plugin loaded: {name} v{plugin.version()}")

    def register_class(self, plugin_cls: type[PluginBase]) -> None:
        """注册插件类（延迟实例化）"""
        # 创建临时实例以获取名称
        temp = plugin_cls.__new__(plugin_cls)
        if hasattr(temp, 'name') and callable(temp.name):
            try:
                name = temp.name()
            except Exception:
                name = plugin_cls.__name__
        else:
            name = plugin_cls.__name__
        self._plugin_classes[name] = plugin_cls
        logger.debug(f"Plugin class registered: {name}")

    def unregister(self, name: str) -> None:
        """卸载插件"""
        if name in self._plugins:
            self._plugins[name].on_unload()
            del self._plugins[name]
            logger.info(f"Plugin unloaded: {name}")

    def get(self, name: str) -> PluginBase | None:
        """获取插件实例（如果是类注册则自动实例化）"""
        if name in self._plugins:
            return self._plugins[name]
        if name in self._plugin_classes:
            plugin = self._plugin_classes[name]()
            self.register(plugin)
            return plugin
        return None

    def get_all(self) -> dict[str, PluginBase]:
        """获取所有已加载的插件"""
        return dict(self._plugins)

    def list_names(self) -> list[str]:
        """列出所有可用的插件名"""
        names = set(self._plugins.keys()) | set(self._plugin_classes.keys())
        return sorted(names)

    def load_from_config(self, config_path: str | Path) -> None:
        """
        从 YAML 配置文件加载插件

        配置格式:
            plugins:
              - module: src.market.indicators.sma
                class: SMAIndicator
                params:
                  period: 20
        """
        config_path = Path(config_path)
        if not config_path.exists():
            logger.warning(f"Plugin config not found: {config_path}")
            return

        with open(config_path) as f:
            config = yaml.safe_load(f)

        plugins_config = config.get("plugins", [])
        for plugin_conf in plugins_config:
            try:
                module_path = plugin_conf["module"]
                class_name = plugin_conf["class"]
                params = plugin_conf.get("params", {})

                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)
                instance = cls(**params) if params else cls()
                self.register(instance)
            except Exception as e:
                logger.error(f"Failed to load plugin {plugin_conf}: {e}")

    def auto_discover(self, package_path: str) -> None:
        """
        自动发现并注册 package 下所有 PluginBase 子类

        Args:
            package_path: 包路径，如 'src.market.indicators'
        """
        try:
            package = importlib.import_module(package_path)
        except ImportError as e:
            logger.error(f"Cannot import package {package_path}: {e}")
            return

        package_dir = Path(inspect.getfile(package)).parent
        for py_file in package_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue
            module_name = f"{package_path}.{py_file.stem}"
            try:
                module = importlib.import_module(module_name)
                for _, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, PluginBase) and obj is not PluginBase:
                        self.register_class(obj)
            except Exception as e:
                logger.debug(f"Skip {module_name}: {e}")
