import time
import mysql.connector

from wordpress_xmlrpc import Client, WordPressPage
from wordpress_xmlrpc.methods import posts


def open_json(fileUrl):
    import json
    import requests
    if fileUrl[0:4] == "http":
        # es URL
        pointer = requests.get(fileUrl)
        return json.loads(pointer.content.decode('utf-8'))
    else:
        # es file
        file = open(fileUrl, "r")
        return json.loads(file.read())

def post_page(wpUrl, wpUserName, wpPassword, articleTitle, articleContent):
    client = Client(wpUrl, wpUserName, wpPassword)
    # Page
    page = WordPressPage()
    page.title = articleTitle
    page.content = articleContent
    # page.terms_names = {'post_tag': articleTags, 'category': articleCategories}
    page.post_status = 'publish'
    # post.thumbnail = attachment_id
    page.id = client.call(posts.NewPost(page))
    print('Post Successfully posted. Its Id is: ', page.id)
    return page.id

# Main parameters

wpUrl = "http://dadesmercatsdiba.cat/xmlrpc.php"

DIBA_USERNAME = "XXX"
DIBA_PASSWORD = "XXX"
DIBA_HOST = "XXX"
DIBA_PORT = "3306"
DIBA_DATABASE = "XXX"
SITE_USER = "XXXX"
SITE_PASS = "XXX"
client = Client(wpUrl, SITE_USER, SITE_PASS)


def clean_custom_fields(postId, pattern):
    # this function removed the previous custom fields of a page
    # specifically designed for removing the CODE_XXX codes necessary for the bokeh visualization
    # this function to run hast to whitelist the IP from where is executed
    HOST = "XXX"
    PORT = "3306"
    DDBB = "XXXX"
    USER = "XXXX"
    PASS = "XXX"

    if postId is None:
        return "wrong id"
    else:
        if pattern is None:
            pattern = "CODE_%"
        elif len(pattern) == 0:
            pattern = "CODE_%"
        # remove the custom fields for the bokeh (previous graphs)
        baseQuery = " DELETE FROM `wp_xrfa_postmeta` WHERE `post_id` = 1045 and `meta_key` like "
        cnx = mysql.connector.connect(
            user=USER,
            password=PASS,
            host=HOST,
            port=PORT,
            database=DDBB
        )
        sqlQuery = baseQuery.replace("1045", str(postId)) + chr(34) + pattern + chr(34)
        print(sqlQuery)
        cursor = cnx.cursor()
        cursor.execute(sqlQuery)
        cnx.commit()
        cnx.close()
        return cursor.rowcount



# connecting to the database


bokehJs = """
    <script type="text/javascript" src="https://cdn.bokeh.org/bokeh/release/bokeh-2.2.1.min.js" integrity="sha384-qkRvDQVAIfzsJo40iRBbxt6sttt0hv4lh74DG7OK4MCHv4C5oohXYoHUM5W11uqS" crossorigin="anonymous"></script>
     <script type="text/javascript">
                Bokeh.set_log_level("info");
            </script>
    """

# retrieve the pages of the mercats
start = 3
end = 132
counter = 0
limit = 300

for marketId in range(start, end):
    if counter > limit:
        break
    else:
        counter += 1
    dataBase = mysql.connector.connect(host=DIBA_HOST, user=DIBA_USERNAME, password=DIBA_PASSWORD, database=DIBA_DATABASE)
    query = "SELECT `mercats`.`id`, `page_id`, `html`, `timestamp`, `mercats`.`nom`, `municipi`, `cens`, `renda_familiar`, `lloguer_mensual`, `index_envelliment`, `dia_mercat` FROM `mercatspages`, `plots`, `mercats`, `municipis` WHERE `mercatspages`.`mercat_id` = `plots`.`added_parameters` AND `mercatspages`.`mercat_id` = `mercats`.`id` AND `municipis`.`nom` = `mercats`.`municipi` AND `mercatspages`.`mercat_id` = " + str(marketId)
    cursorObject = dataBase.cursor()
    cursorObject.execute(query)
    result = cursorObject.fetchall()
    cursorObject.close()
    dataBase.close()

    firstGraph = True
    graphCounter = 0
    customFields = []
    for register in result:  # it will be 3 graphs
        print(register)
        id_ = str(register[0])
        pageId = str(register[1])
        graphHtml = str(register[2])
        timestamp = str(register[3])
        marketName = str(register[4])
        city = str(register[5])
        population = str(register[6])
        familyRent = str(register[7])
        monthlyInstallment = str(register[8])
        aging = str(register[9])
        marketDay = str(register[10])

        newCode = "CODE_main_map_" + str(id_) + "-" + str(graphCounter)
        graphCounter += 1
        if firstGraph:
            content = "La ciutat <b>" + city + "</b> té una població de <b>" + population + "</b> habitants amb un índex d'envelliment de <b>" + aging + "</b><br>"
            content += "La seva renda és de <b>" + familyRent + "</b><br>"
            if len(marketDay) > 1:
                content += "Hi ha mercat <b>" + marketDay + "</b>"
            else:
                content += "No està disponible els dies de mercat"
            content += "<br><br>Aquestes dades són relatives, no absolutes; per a una franja horària donada, l’afluència es calcula en una escala de 0 a 100, on 100 és el moment de més afluència de tota la setmana, i 0 és el moment en què el mercat està buit. <br>Això ens permet realitzar anàlisis comparatives.<br>"
            content += "<br> {{CODE_load_bokeh-" + str(marketId) + "}} "
            content += "{{" + newCode + "}}" + "<br>"
            firstGraph = False
            customFields = [{"key": "CODE_load_bokeh-" + str(marketId), "value": bokehJs}, {"key": newCode, "value": graphHtml}]
        else:
            content += "{{" + newCode + "}}" + "<br>"
            customFields.append({"key": newCode, "value": graphHtml})

        print("el contenido es " + content)

    pageEdited = WordPressPage()
    pageEdited.content = content
    pageEdited.title = marketName
    pageEdited.custom_fields = customFields
    clean_custom_fields(pageId, "")
    client.call(posts.EditPost(pageId, pageEdited))
    time.sleep(30)





