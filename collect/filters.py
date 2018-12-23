from abc import ABC, abstractmethod
from scipy import signal, ndimage
import numpy as np
import pandas as pd


class BaseFilter(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def process(self, data):
        raise NotImplementedError


class NotchFilter(BaseFilter):
    def __init__(self, frequency=1/24., quality=0.05):
        super().__init__()
        self.frequency = frequency
        self.quality = quality

    def process(self, data):
        b, a = signal.iirnotch(self.frequency, self.quality)
        y = abs(signal.filtfilt(b, a, data))
        return y


class Normalize(BaseFilter):
    def __init__(self, max_value=100):
        super().__init__()
        self.max_value = max_value

    def process(self, data):
        return data / np.max(data) * self.max_value


class GaussianFilter(BaseFilter):
    def __init__(self, sigma=1):
        super().__init__()
        self.sigma = sigma

    def process(self, data):
        return ndimage.gaussian_filter1d(data, self.sigma)


class DailyAverage(BaseFilter):
    def __init__(self, hours=24):
        super().__init__()
        self.hours = hours

    def process(self, data):
        series = pd.Series(data)
        series = series.rolling(self.hours).mean()
        return series.values
