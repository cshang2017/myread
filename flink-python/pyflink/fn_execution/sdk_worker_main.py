import os
import sys

# force to register the operations to SDK Harness
from apache_beam.options.pipeline_options import PipelineOptions

try:
    import pyflink.fn_execution.fast_operations
except ImportError:
    import pyflink.fn_execution.operations

# force to register the coders to SDK Harness
import pyflink.fn_execution.coders # noqa # pylint: disable=unused-import

import apache_beam.runners.worker.sdk_worker_main

if 'PIPELINE_OPTIONS' in os.environ:
    pipeline_options = apache_beam.runners.worker.sdk_worker_main._parse_pipeline_options(
        os.environ['PIPELINE_OPTIONS'])
else:
    pipeline_options = PipelineOptions.from_dictionary({})

if __name__ == '__main__':
    apache_beam.runners.worker.sdk_worker_main.main(sys.argv)
