
class MultipleNameFieldFound(Exception):
    def __str__(self):
        return f'More than one field in the model has an is_name_field attribute in metadata'


class NameFieldNotFound(Exception):
    def __str__(self):
        return f'The model does not have a field with an is_name_field attribute in the metadata'
