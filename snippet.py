def safe_eval(expression, variables):
    # Define allowed operators and functions
    allowed_names = {
        'abs': abs,
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.truediv,
        '//': operator.floordiv,
        '%': operator.mod,
        '**': operator.pow,
        '==': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge
    }

    # Add user-defined variables to the allowed context
    for var in variables:
        if var.isidentifier() and var not in allowed_names:
            allowed_names[var] = variables[var]

    # Evaluate the expression using the restricted global context
    return eval(expression, {"__builtins__": None}, allowed_names)
