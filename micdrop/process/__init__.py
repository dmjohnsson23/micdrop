from ..exceptions import StopProcessingException, SkipRowException

def process(sink, *, on_error=None):
    """
    Process the data for the given sink. Yields each final row from the sink.

    :param sink: The sink to process data for
    :param on_error: A function that will be called with any exception raised by the pipeline. The
        function should do one of the following:

        - Return `True`, indicating that the exception was handled successfully and the operation
            should be tried again.
        - Return `False`, indicating that the exception is fatal and should be re-raised.
        - Raise a `SkipRowException` to skip the problematic row and move on to the next.
        - Raise a `StopProcessingException` to gracefully terminate processing.
    """
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
            except Exception as e:
                # An actual error was encountered; allow graceful recovery via on_error function
                if on_error is not None:
                    handled = False
                    try:
                        handled = on_error(e)
                    except SkipRowException:
                        continue
                    except (StopProcessingException, StopIteration):
                        break
                    if handled:
                        # we back up and try again
                        counter -= 1
                        continue
                    else:
                        raise
                else:
                    # No on_error function provided
                    raise

def process_all(sink, return_results=False, *, on_error=None):
    if return_results:
        return list(process(sink, on_error=on_error))
    else:
        for _ in process(sink, on_error=on_error):
            pass