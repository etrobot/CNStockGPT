from .support_factor import calculate_support_factor
from .momentum_factor import calculate_momentum_factor

# Dictionary of all available factors with their calculation functions
AVAILABLE_FACTORS = {
    'support': {
        'func': calculate_support_factor,
        'name': '支撑位'
    },
    'momentum': {
        'func': calculate_momentum_factor,
        'name': '动量'
    }
}

__all__ = ['AVAILABLE_FACTORS']