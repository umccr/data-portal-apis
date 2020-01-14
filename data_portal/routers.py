from rest_framework_nested import routers


class OptionalSlashDefaultRouter(routers.DefaultRouter):
    def __init__(self, *args, **kwargs):
        super(routers.DefaultRouter, self)
        super().__init__(*args, **kwargs)
        self.trailing_slash = '/?'
