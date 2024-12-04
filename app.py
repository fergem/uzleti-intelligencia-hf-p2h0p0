import dash
import dash_mantine_components as dmc
from dash import Dash, _dash_renderer, html, Input, Output, callback, clientside_callback
from dash_iconify import DashIconify
from ploomber.spec import DAGSpec


_dash_renderer._set_react_version("18.2.0")


spec = DAGSpec('etl/pipeline.yaml')
dag = spec.to_dag()

def get_icon(icon):
    return DashIconify(icon=icon, height=16)

app = Dash(__name__, external_stylesheets=dmc.styles.ALL, use_pages=True)

app.layout = dmc.MantineProvider(
    dmc.AppShell(
        [
            dmc.AppShellNavbar([
                dmc.Title(f"Máté's movie reporting page", order=2, bd="xl", c="cyan", ta="center", mb="sm"),
                dmc.Flex(
                    [dmc.Flex(dmc.Button(
                        "Load data from database",
                        id="loading-button",
                        leftSection=DashIconify(icon="fluent:database-plug-connected-20-filled"), w="100%"), mt="auto"),
                     dmc.Flex([dmc.NavLink(
                        label=(page["name"]),
                        leftSection=get_icon(icon="tabler:gauge"),
                        href=page["relative_path"],
                    ) for page in dash.page_registry.values()], direction="column")],
                    direction="column-reverse",
                    h="100%"
                 )
            ],
            p="md"),
            dmc.AppShellMain(children=[html.Div([
                    dash.page_container
                ])],
            ),
        ],
        padding="xl",    
        navbar={
            "padding": "md",
            "width": 300,
            "breakpoint": "sm",
            "collapsed": {"mobile": True},
        },
    )
)

clientside_callback(
    """
    function updateLoadingState(n_clicks) {
        return true
    }
    """,
    Output("loading-button", "loading", allow_duplicate=True),
    Input("loading-button", "n_clicks"),
    prevent_initial_call=True,
)

@callback(
    Output("loading-button", "loading"),
    Input("loading-button", "n_clicks"),
    prevent_initial_call=True,
)
def load_from_db(n_clicks):
    if n_clicks > 0:
        try:
            dag.build(force=True)
            return False
        except Exception as e:
            print(e)
            return False
    return False
    
if __name__ == "__main__":
    app.run(debug=True)