# -*- coding: utf-8 -*-
"""domain package

You may wish to model Domain objects here. Domain class (object) are not always necessary to be
persistence entities (i.e. Django ORM models).

Think of Struct (ala C/Rust)! Or, some template Class or complex data type (ala Java) that you'd like to model.
These could just be DTO (Data Transfer Object) or data carrier (ala Plain Object Java Object _POJO_ manner) instances.
Or, also can contain lot of methods and functions to enrich Domain behaviour.

Please note that in Python, there isn't strict rule or clear distinction on how to approach modelling a Domain problem.
You can organise module and functions (module-oriented) way or, class and functions (object-oriented) way!
For example, in "orchestration" package, it is organised into step modules! We could change this to be OOP.

With duck typing and, in Python we can't effectively protect or private something. Information hiding is at the hand of
coder and, mutation is one own discipline! We could adopt either way or both way. This is flexibility in Python and, it
could be good or THIS IS THE WAY!
"""
