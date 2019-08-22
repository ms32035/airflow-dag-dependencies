import os

from airflow import conf, models, settings
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from flask import render_template, request
from flask_appbuilder import BaseView, has_access, expose

dagbag = models.DagBag(settings.DAGS_FOLDER)


class DAGDependenciesView(BaseView):
    plugins_folder = conf.get("core", "plugins_folder")
    template_folder = os.path.join(plugins_folder, "dag-dependencies-plugin")
    route_base = "/"

    def render(self, template, **context):
        return render_template(
            template,
            base_template=self.appbuilder.base_template,
            appbuilder=self.appbuilder,
            **context,
        )

    @expose("/dag-dependencies")
    @has_access
    def list(self):
        title = "DAG Dependencies"

        nodes = []
        edges = []

        def _node_dict(node_id, label, style):
            return {
                "id": node_id,
                "value": {"label": label, "style": style, "rx": 5, "ry": 5},
            }

        for dag_id, dag in dagbag.dags.items():
            dag_node_id = "[d]" + dag_id
            nodes.append(_node_dict(dag_node_id, dag_id, "fill: rgb(232, 247, 228)"))

            for task in dag.tasks:
                task_node_id = "[t]" + dag_id + "#" + task.task_id
                if isinstance(task, TriggerDagRunOperator):
                    nodes.append(
                        _node_dict(
                            task_node_id, task.task_id, "fill: rgb(255, 239, 235)"
                        )
                    )
                    edges.append({"u": dag_node_id, "v": task_node_id})
                    edges.append({"u": task_node_id, "v": "[d]" + task.trigger_dag_id})
                elif isinstance(task, ExternalTaskSensor):
                    nodes.append(
                        _node_dict(
                            task_node_id, task.task_id, "fill: rgb(230, 241, 242)"
                        )
                    )
                    edges.append({"u": task_node_id, "v": dag_node_id})
                    edges.append({"u": "[d]" + task.external_dag_id, "v": task_node_id})

        return self.render_template(
            "dag_dependencies.html",
            title=title,
            nodes=nodes,
            edges=edges,
            arrange=conf.get("webserver", "dag_orientation"),
            width=request.args.get("width", "100%"),
            height=request.args.get("height", "800"),
        )


v_appbuilder_view = DAGDependenciesView()
v_appbuilder_package = {
    "name": "DAG Dependencies",
    "category": "Browse",
    "view": v_appbuilder_view,
}


class DAGDependenciesPlugin(AirflowPlugin):
    name = "dag_dependencies_plugin"
    appbuilder_views = [v_appbuilder_package]
