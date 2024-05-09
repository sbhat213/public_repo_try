class AddProcessConfigurationCommand:

    def __init__(self, name, steps, depends_on, global_map):
        self.name = name
        self.steps = steps
        self.depends_on = depends_on
        self.global_map = global_map
