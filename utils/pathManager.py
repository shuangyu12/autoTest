from individualStockReview.core.paths import PathManager


def get_path_manager(runtime_config=None):
    manager = PathManager(runtime_config=runtime_config)
    manager.ensure_directories()
    return manager


__all__ = ["PathManager", "get_path_manager"]
