from pyflink.java_gateway import get_gateway

__all__ = ['InputDependencyConstraint']


class InputDependencyConstraint(object):
    """
    This constraint indicates when a task should be scheduled considering its inputs status.

    :data:`ANY`:

    Schedule the task if any input is consumable.

    :data:`ALL`:

    Schedule the task if all the inputs are consumable.
    """

    ANY = 0
    ALL = 1

    @staticmethod
    def _from_j_input_dependency_constraint(j_input_dependency_constraint):
        gateway = get_gateway()
        JInputDependencyConstraint = gateway.jvm.org.apache.flink.api.common \
            .InputDependencyConstraint
        if j_input_dependency_constraint == JInputDependencyConstraint.ANY:
            return InputDependencyConstraint.ANY
        elif j_input_dependency_constraint == JInputDependencyConstraint.ALL:
            return InputDependencyConstraint.ALL
        else:
            raise Exception("Unsupported java input dependency constraint: %s"
                            % j_input_dependency_constraint)

    @staticmethod
    def _to_j_input_dependency_constraint(input_dependency_constraint):
        gateway = get_gateway()
        JInputDependencyConstraint = gateway.jvm.org.apache.flink.api.common \
            .InputDependencyConstraint
        if input_dependency_constraint == InputDependencyConstraint.ANY:
            return JInputDependencyConstraint.ANY
        elif input_dependency_constraint == InputDependencyConstraint.ALL:
            return JInputDependencyConstraint.ALL
        else:
            raise TypeError("Unsupported input dependency constraint: %s, supported input "
                            "dependency constraints are: InputDependencyConstraint.ANY and "
                            "InputDependencyConstraint.ALL." % input_dependency_constraint)
