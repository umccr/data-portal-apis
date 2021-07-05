# -*- coding: utf-8 -*-
"""orchestration package

This typically compliment Orchestrator act upon a particular event to PERFORM some step(s).
Typically contain X_step modules file that implement "perform()" interface for a particular step in the pipeline.

Very Domain-rich impls!
Domain logic are mainly concentrated here. After domain logic are sorted here in X_step module, you use
service layer X_srv(s) to interface with RDBMS for persisting their state in database and/or notifying to Slack.

Not limited only to Orchestrator!
The whole point we do here is Genomic Workflow "orchestration" that is the key domain context.
Orchestrator (lambda) is the key actor. But other actors from lambdas package _steps_ are welcome here too!
"""
