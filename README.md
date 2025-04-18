# airflow
This repo contains learning material and working hands on code to practice.

# Apache Airflow
Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow’s extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single process on your laptop to a distributed setup to support even the biggest workflows.

# Dynamic Airflow DAGs

In Airflow, DAGs are defined as Python code. Airflow executes all Python code in the dags_folder and loads any DAG objects that appear in globals(). The simplest way to create a DAG is to write it as a static Python file.

Sometimes, manually writing DAGs isn't practical. Maybe you have hundreds or thousands of DAGs that do similar things with just a parameter changing between them. Or maybe you need a set of DAGs to load tables, but don't want to manually update DAGs every time the tables change. In these cases, and others, it makes more sense to dynamically generate DAGs.

Because everything in Airflow is code, you can dynamically generate DAGs using Python alone. As long as a DAG object in globals() is created by Python code that is stored in the dags_folder, Airflow will load it. In this guide, you'll learn how to dynamically generate DAGs. You'll learn when DAG generation is the preferred option and what pitfalls to avoid
