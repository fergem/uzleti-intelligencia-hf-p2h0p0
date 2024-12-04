from ploomber.spec import DAGSpec


spec = DAGSpec('etl/pipeline.yaml')
dag = spec.to_dag()
print(dag.status())
dag.build(force=True)