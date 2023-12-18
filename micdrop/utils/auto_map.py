def automap_known_keys(source, sink, exclude_keys=[]):
    for key in source.key():
        if key not in exclude_keys:
            source.take(key) >> sink.put(key)