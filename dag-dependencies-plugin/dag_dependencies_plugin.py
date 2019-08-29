import os
from datetime import datetime, timedelta

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
    refresh_interval = conf.getint(
        "dag_dependencies_plugin", "refresh_interval", fallback=300
    )
    last_refresh = datetime(2000, 1, 1)
    nodes = []
    edges = []

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

        if datetime.utcnow() > self.last_refresh + timedelta(
            seconds=self.refresh_interval
        ):
            dagbag.collect_dags()
            self.nodes, self.edges = self._generate_graph()
            self.last_refresh = datetime.utcnow()

        return self.render_template(
            "dag_dependencies.html",
            title=title,
            nodes=self.nodes,
            edges=self.edges,
            last_refresh=self.last_refresh.strftime("%Y-%m-%d %H:%M:%S"),
            arrange=conf.get("webserver", "dag_orientation"),
            width=request.args.get("width", "100%"),
            height=request.args.get("height", "800"),
        )

    @staticmethod
    def _generate_graph():
        nodes = {}
        edges = []

        for dag_id, dag in dagbag.dags.items():
            dag_node_id = "[d]" + dag_id
            nodes[dag_node_id] = DAGDependenciesView._node_dict(
                dag_node_id, dag_id, "fill: rgb(232, 247, 228)"
            )

            for task in dag.tasks:
                task_node_id = "[t]" + dag_id + "#" + task.task_id
                if isinstance(task, TriggerDagRunOperator):
                    nodes[task_node_id] = DAGDependenciesView._node_dict(
                        task_node_id, task.task_id, "fill: rgb(255, 239, 235)"
                    )

                    edges.append({"u": dag_node_id, "v": task_node_id})
                    edges.append({"u": task_node_id, "v": "[d]" + task.trigger_dag_id})
                elif isinstance(task, ExternalTaskSensor):
                    nodes[task_node_id] = DAGDependenciesView._node_dict(
                        task_node_id, task.task_id, "fill: rgb(230, 241, 242)"
                    )

                    edges.append({"u": task_node_id, "v": dag_node_id})
                    edges.append({"u": "[d]" + task.external_dag_id, "v": task_node_id})

            implicit = getattr(dag, "implicit_dependencies", None)
            if isinstance(implicit, list):
                for dep in implicit:
                    dep_node_id = "[i]" + dag_id + "#" + dep
                    nodes[dep_node_id] = DAGDependenciesView._node_dict(
                        dep_node_id, "implicit", "fill: gold"
                    )

                    edges.append({"u": dep_node_id, "v": dag_node_id})
                    edges.append({"u": "[d]" + dep, "v": dep_node_id})

        return list(nodes.values()), edges

    @staticmethod
    def _node_dict(node_id, label, style):
        return {
            "id": node_id,
            "value": {"label": label, "style": style, "rx": 5, "ry": 5},
        }


v_appbuilder_view = DAGDependenciesView()
v_appbuilder_package = {
    "name": "DAG Dependencies",
    "category": "Browse",
    "view": v_appbuilder_view,
}


class DAGDependenciesPlugin(AirflowPlugin):
    name = "dag_dependencies_plugin"
    appbuilder_views = [v_appbuilder_package]
