import mysql.connector
import pandas as pd
from os.path import join, dirname
from dotenv import load_dotenv

from bokeh.io import output_file, save

from bokeh.plotting import figure

from bokeh.embed import file_html
from bokeh.resources import CDN
from bokeh.models import Title

from database_operations import *


def echo(concept, variable):
    print("*** " + concept + " ***")
    print(variable)
    print("--- " + concept + " ---")


def load_indiv_market_all_time2(id_mercat, cnx):
    df = pd.read_sql(f"""
        SELECT 
            `mercat_id`, `day_of_week`, `hour`, AVG(`value`) as afluence
        FROM 
            populartimes 
        WHERE 
            `mercat_id` = \"{id_mercat}\"
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        ORDER BY
            `day_of_week`, `hour` ASC
        """,
                     con=cnx)
    return df


def load_indiv_market_last_week2(id_mercat, cnx):
    df = pd.read_sql(f"""
       SELECT 
            `mercat_id`, `day_of_week`, `hour`, `value` as afluence, max(timestamp) as timestamp
       FROM  
            populartimes 
        WHERE 
            `mercat_id` = \"{id_mercat}\" AND
            TIMESTAMPDIFF(WEEK, `timestamp`, CURRENT_TIMESTAMP()) BETWEEN 1 AND 2 
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        ORDER BY
            `day_of_week`, `hour` ASC
        """,
                     con=cnx)
    return df




def load_indiv_market_this_week2(id_mercat, cnx):
    df = pd.read_sql(f"""
       SELECT 
            `mercat_id`, `day_of_week`, `hour`, `value` as afluence, max(timestamp) as timestamp
       FROM  
            populartimes 
        WHERE 
            `mercat_id` =  \"{id_mercat}\"
        GROUP BY 
            mercat_id, `day_of_week`, `hour`
        ORDER BY
            `day_of_week`, `hour` ASC
        """,
                     con=cnx)
    return df




dotenv_path = join(dirname(__file__), '../.env')
load_dotenv(dotenv_path)

# DIBA_USERNAME = os.environ.get("DIBA_USERNAME")
# DIBA_PASSWORD = os.environ.get("DIBA_PASSWORD")
# DIBA_HOST = os.environ.get("DIBA_HOST")
# DIBA_PORT = os.environ.get("DIBA_PORT")
# DIBA_DATABASE = os.environ.get("DIBA_DATABASE")
# Load env variables for database connection
DIBA_USERNAME = "XXX"
DIBA_PASSWORD = "XXXX"
DIBA_HOST = "XXXX"
DIBA_PORT = "3306"
DIBA_DATABASE = "XXXX"

cnx = mysql.connector.connect(
    user=DIBA_USERNAME,
    password=DIBA_PASSWORD,
    host=DIBA_HOST,
    port=DIBA_PORT,
    database=DIBA_DATABASE
)

mercats_indiv = load_indiv_market_data(cnx)

cnx.close()

#######################################################################################

# CONFIGURATIONS

# Variable to store all plots, so that we can INSERT them at the end
plots = []

# Visual configurations
fill_color = "#D72525"
palette = ["#D72525", "#56423E", "#BEA6A1", "#008B90", "#00C3C6", "#CCC8AD", "#FF9F86"]

#######################################################################################

# TOTS ELS GRÀFICS INDIVIDUAL DE MERCAT (PER A CADA MERCAT)

count = 0
limit = 200
for index, row in mercats_indiv.iterrows():
    if count > limit:
        break
    else:
        count += 1

    id_mercat_indiv = row['id']  # Used to save plot to DB
    nom_mercat_indiv = row['nom']  # Used for load_indiv_market_* functions

    ######################## AÑADIDO: SOLUCIONAR MERCADOS SIN DATOS ########################
    if nom_mercat_indiv in ["Mercat Municipal SIN DATOS 1", "Mercat Municipal SIN DATOS 2",
                            "Mercat Municipal SIN DATOS 3"]:
        plots.append(("individual", id_mercat_indiv, "<p>Dades no disponibles per aquest mercat.</p>"))
        # Esto se escribirá en write_plots y por tanto en la BBDD, así que cuando se generen las
        # páginas saldrá el HTML por pantalla en la página que toca.
        continue
    ################################## FIN DEL AÑADIDO ###########################################

    print(f"Mercat: {nom_mercat_indiv}")
    cnx = mysql.connector.connect(
        user=DIBA_USERNAME,
        password=DIBA_PASSWORD,
        host=DIBA_HOST,
        port=DIBA_PORT,
        database=DIBA_DATABASE,
        charset='utf8'
    )
    indiv_market_this_week = load_indiv_market_this_week2(id_mercat_indiv, cnx)
    indiv_market_last_week = load_indiv_market_last_week2(id_mercat_indiv, cnx)
    indiv_market_all_time = load_indiv_market_all_time2(id_mercat_indiv, cnx)
    cnx.close()

    days_of_week = ["Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte", "Diumenge"]

    # GRÀFIC INDIVIDUAL DE MERCAT: POPULAR TIMES AQUESTA SETMANA

    # Line plot HTML will be written in output_file (see last line)
    output_file("../plots/lineplot_indiv_mercat_this_week.html")

    x = [i for i in range(6, 24)]
    dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [], [], [], [], [], [], []

    # in case there is no data it's filled with zeroes
    if indiv_market_this_week.empty:
        dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [[0 for _ in range(6, 24)] for _ in range(0, len(days_of_week))]
    else:
        dilluns =   [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Dilluns"]
        dimarts =   [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Dimarts"]
        dimecres =  [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Dimecres"]
        dijous =    [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Dijous"]
        divendres = [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Divendres"]
        dissabte =  [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Dissabte"]
        diumenge =  [row['afluence'] for index, row in indiv_market_this_week.iterrows() if row['day_of_week'] == "Diumenge"]

    # Instantiate the figure object
    graph = figure(
        title=f"Afluència aquesta setmana en el {nom_mercat_indiv}",
        x_axis_label="Hora del dia (06:00 - 23:00)",
        y_axis_label="Afluència",
        toolbar_location=None
    )
    echo("x", x)
    echo("dilluns", dilluns)
    graph.line(x, dilluns, color=palette[0], line_width=2, legend_label="Dilluns")
    graph.line(x, dimarts, color=palette[1], line_width=2, legend_label="Dimarts")
    graph.line(x, dimecres, color=palette[2], line_width=2, legend_label="Dimecres")
    graph.line(x, dijous, color=palette[3], line_width=2, legend_label="Dijous")
    graph.line(x, divendres, color=palette[4], line_width=2, legend_label="Divendres")
    graph.line(x, dissabte, color=palette[5], line_width=2, legend_label="Dissabte")
    graph.line(x, diumenge, color=palette[6], line_width=2, legend_label="Diumenge")

    graph.legend.location = "top_right"
    graph.legend.click_policy = "hide"
    graph.title.text_font_size = "16px"
    graph.title.text_font = "Arial"
    graph.add_layout(Title(text="Clic a la llegenda per activar/desactivar dies", align="left"), "right")
    graph.xaxis.ticker = list(range(6, 24))

    # Write HTML to output_file
    # save(graph)

    # Write HTML to variable and save to database
    html = file_html(graph, CDN)
    html = html.split("<body>")[1].split("</body>")[0].strip()
    plots.append(("individual", id_mercat_indiv, html))

    # GRÀFIC INDIVIDUAL DE MERCAT: POPULAR TIMES LA SETMANA PASSADA

    # Line plot HTML will be written in output_file (see last line)
    output_file("../plots/lineplot_indiv_mercat_last_week.html")

    x = [i for i in range(6, 24)]
    dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [], [], [], [], [], [], []

    if indiv_market_last_week.empty:
        dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [[0 for _ in range(0, 18)] for _ in
                                                                             range(0, len(days_of_week))]
    else:
        dilluns =   [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Dilluns"]
        dimarts =   [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Dimarts"]
        dimecres =  [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Dimecres"]
        dijous =    [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Dijous"]
        divendres = [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Divendres"]
        dissabte =  [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Dissabte"]
        diumenge =  [row['afluence'] for index, row in indiv_market_last_week.iterrows() if row['day_of_week'] == "Diumenge"]

    # Instantiate the figure object
    graph = figure(
        title=f"Afluència la setmana passada en el {nom_mercat_indiv}",
        x_axis_label="Hora del dia (06:00 - 23:00)",
        y_axis_label="Afluència",
        toolbar_location=None
    )

    graph.line(x, dilluns, color=palette[0], line_width=2, legend_label="Dilluns")
    graph.line(x, dimarts, color=palette[1], line_width=2, legend_label="Dimarts")
    graph.line(x, dimecres, color=palette[2], line_width=2, legend_label="Dimecres")
    graph.line(x, dijous, color=palette[3], line_width=2, legend_label="Dijous")
    graph.line(x, divendres, color=palette[4], line_width=2, legend_label="Divendres")
    graph.line(x, dissabte, color=palette[5], line_width=2, legend_label="Dissabte")
    graph.line(x, diumenge, color=palette[6], line_width=2, legend_label="Diumenge")

    graph.legend.location = "top_right"
    graph.legend.click_policy = "hide"
    graph.title.text_font_size = "16px"
    graph.title.text_font = "Arial"
    graph.add_layout(Title(text="Clic a la llegenda per activar/desactivar dies", align="left"), "right")
    graph.xaxis.ticker = list(range(6, 24))

    # Write HTML to output_file
    # save(graph)

    # Write HTML to variable and save to database
    html = file_html(graph, CDN)
    html = html.split("<body>")[1].split("</body>")[0].strip()
    plots.append(("individual", id_mercat_indiv, html))

    # GRÀFIC INDIVIDUAL DE MERCAT: HISTÒRIC

    # Line plot HTML will be written in output_file (see last line)
    output_file("../plots/lineplot_indiv_mercat_all_time.html")

    x = [i for i in range(6, 24)]
    dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [], [], [], [], [], [], []

    if indiv_market_all_time.empty:
        dilluns, dimarts, dimecres, dijous, divendres, dissabte, diumenge = [[0 for _ in range(0, 18)] for _ in
                                                                             range(0, len(days_of_week))]
    else:
        dilluns =   [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Dilluns"]
        dimarts =   [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Dimarts"]
        dimecres =  [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Dimecres"]
        dijous =    [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Dijous"]
        divendres = [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Divendres"]
        dissabte =  [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Dissabte"]
        diumenge =  [row['afluence'] for index, row in indiv_market_all_time.iterrows() if row['day_of_week'] == "Diumenge"]

    # Instantiate the figure object
    graph = figure(
        title=f"Afluència mitjana històrica en el {nom_mercat_indiv}",
        x_axis_label="Hora del dia (06:00 - 23:00)",
        y_axis_label="Afluència",
        toolbar_location=None
    )

    graph.line(x, dilluns, color=palette[0], line_width=2, legend_label="Dilluns")
    graph.line(x, dimarts, color=palette[1], line_width=2, legend_label="Dimarts")
    graph.line(x, dimecres, color=palette[2], line_width=2, legend_label="Dimecres")
    graph.line(x, dijous, color=palette[3], line_width=2, legend_label="Dijous")
    graph.line(x, divendres, color=palette[4], line_width=2, legend_label="Divendres")
    graph.line(x, dissabte, color=palette[5], line_width=2, legend_label="Dissabte")
    graph.line(x, diumenge, color=palette[6], line_width=2, legend_label="Diumenge")

    graph.legend.location = "top_right"
    graph.legend.click_policy = "hide"
    graph.title.text_font_size = "16px"
    graph.title.text_font = "Arial"
    graph.add_layout(Title(text="Clic a la llegenda per activar/desactivar dies", align="left"), "right")
    graph.xaxis.ticker = list(range(6, 24))

    # Write HTML to output_file
    # save(graph)

    # Write HTML to variable and save to database
    html = file_html(graph, CDN)
    html = html.split("<body>")[1].split("</body>")[0].strip()
    plots.append(("individual", id_mercat_indiv, html))

#######################################################################################

# WRITE ALL PLOTS TO THE DATABASE

write_plots(plots)
