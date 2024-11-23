import dash
import dash_mantine_components as dmc
from dash import Dash, _dash_renderer, dcc, html
from dash_iconify import DashIconify

_dash_renderer._set_react_version("18.2.0")

def get_icon(icon):
    return DashIconify(icon=icon, height=16)

app = Dash(__name__, external_stylesheets=dmc.styles.ALL, use_pages=True)

app.layout = dmc.MantineProvider(
    dmc.AppShell(
        [
            dmc.AppShellNavbar([
                dmc.Title(f"Máté's movie reporting page", order=2, bd="xl", c="cyan", ta="center", mb="sm"),
                dmc.Flex(
                    [dmc.NavLink(
                        label=(page["name"]),
                        leftSection=get_icon(icon="tabler:gauge"),
                        href=page["relative_path"],
                    ) for page in dash.page_registry.values()],
                    direction="column",
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

if __name__ == "__main__":
    app.run(debug=True)