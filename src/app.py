import dash
from dash import html
from dash import dcc
import os
import base64


REPORTS_DIR ="../reports"

app = dash.Dash(__name__)

def encode_image(image_path):
    """Codifica una imagen en base64 para poder mostrarla en HTML."""
    if not os.path.exists(image_path):
        print(f"Advertencia: La imagen no se encontró en {image_path}")
        return None
    with open(image_path, 'rb') as f:
        encoded_image = base64.b64encode(f.read()).decode('ascii')
    return f"data:image/png;base64,{encoded_image}"


app.layout = html.Div(children=[
    html.H1(children='Dashboard de Análisis de Películas', style={'textAlign': 'center', 'color': '#503D36'}),
    html.Div(children='Visualizaciones de películas por región y país.', style={'textAlign': 'center', 'color': '#503D36'}),
    html.Hr(), 
    html.H2(children=f'Análisis para la Región de Interés (UY)', style={'textAlign': 'left', 'color': '#503D36', 'margin-top': '20px', 'margin-left': '20px'}),
    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Top 10 Géneros más Populares en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'genre_popularity_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Distribución de Ratings en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'rating_distribution_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='twelve columns', children=[
            html.H3('Número de Películas Estrenadas por Año por Género (UY)'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'year_genre_trends_UY.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ]),

    html.Hr(),
    html.H2(children='Análisis Adicional para Uruguay (UY)', style={'textAlign': 'left', 'color': '#503D36', 'margin-top': '20px', 'margin-left': '20px'}),
    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Distribución de Duración de Películas en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'runtime_distribution_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Rating Promedio por Género en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'avg_rating_by_genre_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Correlación entre Votos y Rating en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'votes_vs_rating_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Top 10 Directores en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_directors_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Top 10 Actores/Actrices en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_actors_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Top 15 Películas por Potencial de Ingresos en UY'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_revenue_potential_UY.png')), style={'width': '80%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='twelve columns', children=[
            html.H3('Tendencias de Popularidad de Géneros en Uruguay (Últimos 20 Años)'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'genre_trends_20years_UY.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ]),

    html.Hr(), 

    html.H2(children='Top 30 Películas Más Vistas por País (Últimos 25 Años)', style={'textAlign': 'left', 'color': '#503D36', 'margin-top': '20px', 'margin-left': '20px'}),

 
    html.Div(className='row', children=[
        html.Div(className='six columns', children=[ 
            html.H3('Top 30 US'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_30_movies_US.png')), style={'width': '100%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Top 30 AR'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_30_movies_AR.png')), style={'width': '100%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Top 30 ES'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_30_movies_ES.png')), style={'width': '100%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Top 30 FR'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'top_30_movies_FR.png')), style={'width': '100%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Hr(), 

    html.H2(children='Tendencias de Películas por Año y Género por País', style={'textAlign': 'left', 'color': '#503D36', 'margin-top': '20px', 'margin-left': '20px'}),
    
    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Tendencias Género US'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'year_genre_trends_US.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Tendencias Género AR'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'year_genre_trends_AR.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'}),

    html.Div(className='row', children=[
        html.Div(className='six columns', children=[
            html.H3('Tendencias Género ES'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'year_genre_trends_ES.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'}),
        html.Div(className='six columns', children=[
            html.H3('Tendencias Género FR'),
            html.Img(src=encode_image(os.path.join(REPORTS_DIR, 'year_genre_trends_FR.png')), style={'width': '90%', 'height': 'auto', 'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'})
        ], style={'padding': '20px'})
    ], style={'display': 'flex'})
])

if __name__ == '__main__':
    print(f"Buscando imágenes en el directorio: {os.path.abspath(REPORTS_DIR)}")
    app.run_server(debug=True)