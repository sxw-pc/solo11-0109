from datetime import datetime
def aware(s):
    value=datetime.fromisoformat(s.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('timezone_required')
    return value
def validate_window(window):
    if not aware(window['start_at'])<aware(window['end_at'])<=aware(window['cutoff_at']):raise ValueError('window_bounds')
def validate_rate(rate):
    if not aware(rate['start_at'])<aware(rate['end_at']):raise ValueError('rate_bounds')
