import six as _six

from flytekit.common.types.helpers import get_sdk_type_from_literal_type as _get_sdk_type_from_literal_type
from flytekit.models import literals as _literals


def construct_literal_map_from_variable_map(variable_dict, text_args):
    """
    This function produces a map of Literals to use when creating an execution.  It reads the required values from
    a Variable map (presumably obtained from a launch plan), and then fills in the necessary inputs
    from the click args. Click args will be strings, which will be parsed into their SDK types with each
    SDK type's parse string method.

    :param dict[Text, flytekit.models.interface.Variable] variable_dict:
    :param dict[Text, Text] text_args:
    :rtype: flytekit.models.literals.LiteralMap
    """
    inputs = {}

    for var_name, variable in _six.iteritems(variable_dict):
        # Check to see if it's passed from click
        # If this is an input that has a default from the LP, it should've already been parsed into a string,
        # and inserted into the default for this option, so it should still be here.
        if var_name in text_args and text_args[var_name] is not None:
            # the SDK type is also available from the sdk workflow object's saved user inputs but let's
            # derive it here from the interface to be more rigorous.
            sdk_type = _get_sdk_type_from_literal_type(variable.type)
            inputs[var_name] = sdk_type.from_string(text_args[var_name])

    return _literals.LiteralMap(literals=inputs)


def parse_args_into_dict(input_arguments):
    """
    Takes a tuple like (u'input_b=mystr', u'input_c=18') and returns a dictionary of input name to the
    original string value

    :param Tuple[Text] input_arguments:
    :rtype: dict[Text, Text]
    """

    return {split_arg[0]: split_arg[1] for split_arg in [input_arg.split("=", 1) for input_arg in input_arguments]}


def construct_literal_map_from_parameter_map(parameter_map, text_args):
    """
    Take a dictionary of Text to Text and construct a literal map using a ParameterMap as guidance.
    Required input parameters must have an entry in the text arguments given.
    Parameters with defaults will have those defaults filled in if missing from the text arguments.

    :param flytekit.models.interface.ParameterMap parameter_map:
    :param dict[Text, Text] text_args:
    :rtype: flytekit.models.literals.LiteralMap
    """

    # This function can be written by calling construct_literal_map_from_variable_map also, but not that much
    # code is saved.
    inputs = {}
    for var_name, parameter in _six.iteritems(parameter_map.parameters):
        sdk_type = _get_sdk_type_from_literal_type(parameter.var.type)
        if parameter.required:
            if var_name in text_args and text_args[var_name] is not None:
                inputs[var_name] = sdk_type.from_string(text_args[var_name])
            else:
                raise Exception("Missing required parameter {}".format(var_name))
        else:
            if var_name in text_args and text_args[var_name] is not None:
                inputs[var_name] = sdk_type.from_string(text_args[var_name])
            else:
                inputs[var_name] = parameter.default

    return _literals.LiteralMap(literals=inputs)


def str2bool(str):
    """
    bool('False') is True in Python, so we need to do some string parsing.  Use the same words in ConfigParser
    :param Text str:
    :rtype: bool
    """
    return not str.lower() in ["false", "0", "off", "no"]
