import numpy as np
import warnings
import sys


def apply_numpy_compatibility_patch():
    """应用NumPy兼容性补丁"""
    if not hasattr(np, 'bool8'):
        np.bool8 = np.bool_
    if not hasattr(np, 'object0'):
        np.object0 = np.object_
    if not hasattr(np, 'int0'):
        np.int0 = np.int_
    if not hasattr(np, 'uint0'):
        np.uint0 = np.uint
    if not hasattr(np, 'float96'):
        np.float96 = np.longdouble
    if not hasattr(np, 'float128'):
        np.float128 = np.longdouble

    if not hasattr(np, 'unicode_'):
        np.unicode_ = np.str_
    if not hasattr(np, 'bytes_'):
        np.bytes_ = np.bytes0 = np.bytes

    if not hasattr(np, 'round_'):
        np.round_ = np.round

    try:
        import os
        os.environ['NPY_PROMOTION_STATE'] = 'weak'
    except:
        pass

    warnings.filterwarnings('ignore', category=DeprecationWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=UserWarning)

    print("✅ NumPy兼容性补丁已应用")


apply_numpy_compatibility_patch()
