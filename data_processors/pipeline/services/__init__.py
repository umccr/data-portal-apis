# -*- coding: utf-8 -*-
"""services package

This package contain interfacing with external systems such as Slack messaging, RDBMS and
data access layer impls; to and from database through Django ORM models.

It also serve as transactional boundary. If you need to persist into Portal database then
probably it should live within this package. Hence, please avoid doing SomeDomainModel.save()
in elsewhere and/or in Controller layer like lambdas package.

It may also contains Business logic to some extent as we retrieve or persist into database. However;
When context is cleared, you should organise those domain logic into X_step modules in orchestration package.
"""
