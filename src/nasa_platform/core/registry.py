import functools

JOB_REGISTRY = {}

def register_job(name: str):
    def decorator(cls):
        JOB_REGISTRY[name] = cls
        return cls
      #  @functools.wraps(cls)
      #  def wrapper(*args, **kwargs):
      #      return cls(*args, **kwargs)
      #  return wrapper
    return decorator