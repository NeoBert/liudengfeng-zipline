import numpy as np
from zipline.utils.numpy_utils import int64_dtype
from .classifiers import Classifier
from .fundamentals import Fundamentals

# TODO:改写
# class Sector(Classifier):
#     dtype = int64_dtype
#     window_length = 0
#     inputs = ()
#     missing_value = -1

#     def __init__(self):
#         self.data = np.load('../../data/project_4_sector/data.npy')

#     def _compute(self, arrays, dates, assets, mask):
#         return np.where(
#             mask,
#             self.data[assets],
#             self.missing_value,
#         )