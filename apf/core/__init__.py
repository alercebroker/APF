import importlib


def get_class(string):
    """Import class from string.

    Parameters
    ----------
    string : str
        absolute python import path for the class. (i.e. apf.consumers.GenericConsumer)

    Returns
    -------
    object
        Class imported.

    """
    module_name, class_name = string.rsplit(".", 1)
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_
