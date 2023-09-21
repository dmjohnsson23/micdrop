from ..exceptions import StopProcessingException, SkipRowException

def process(sink):
    counter = 0
    with sink.opened:
        while True:
            counter += 1
            try:
                sink.idempotent_next(counter)
                yield sink.get()
            except SkipRowException:
                continue
            except (StopProcessingException, StopIteration):
                break

def process_all(sink, return_results=False):
    if return_results:
        return list(process(sink))
    else:
        for _ in process(sink):
            pass