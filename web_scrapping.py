import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests 
import csv 
import time


#-----------------------------------------------GLOBAL VARIABLES BEGIN---------------------------------------------------
NUMBER_PAGES: int = 0
RESTAURANT_ID: int = 1
#Constant variables
BASE_URL: str = "https://guide.michelin.com/"
CSV_DIRECTION: str = "Web Scrapping/restaurants_dataset.csv"
FIELD_NAMES: list[str] = [
    "id",
    "name",
    "types of restaurants",
    "country",
    "state",
    "zip code",
    "distinction",
    "sustainability",
]
DISTINCTION_NAMES: list[str] = ["Bib Gourmand", "One Star", "Two Stars:", "Green Star"]
#-----------------------------------------------GLOBAL VARIABLES END---------------------------------------------------

#-----------------------------------------------FUNCTIONS BEGING---------------------------------------------------
def get_html_content(url: str) -> BeautifulSoup:
    """This function returns the HTML content of a URL
    
    Args:
        url (str): The URL of the website to scrape.

    Returns:
        BeautifulSoup: The parsed HTML structure of the website.
    """
    return BeautifulSoup(requests.get(url).content, "html.parser")

def scrape_html_component(html: BeautifulSoup, find: str, component: str, classs_: str) -> BeautifulSoup:
    """This function scrapes a specific component of an HTML document with a specified class.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the website.
        find (str): The method to use for finding elements ("find" to get the first occurrence, "find_all" to get all occurrences).
        component (str): The HTML tag (e.g., "div", "span", "p") to search for.
        classs_ (str): The class name of the component to search for.

    Returns:
        BeautifulSoup object or list[BeautifulSoup object]: The HTML content of the component(s) that match the criteria.
    """
    return getattr(html, find)(component, class_=classs_)

def extract_url_pages(html_component: list[BeautifulSoup]) -> list[str]:
    """Extracts a list of URLs from a list of HTML components.

    Args:
        html_component (list[BeautifulSoup]): A list of BeautifulSoup objects representing the HTML components to scrape.

    Returns:
        list[str]: A list of URLs (strings) extracted from the "href" attribute of the provided HTML components.
    """
    return [links["href"] for links in html_component if "href" in links.attrs]

def write_csv(file_direction: str, mode: str, write_headers_or_rows: bool, data: list[dict] ) -> None:  
    """Creates or appends restaurant data to a CSV file.

    Args:
        mode (str): File opening mode:
                   - 'w': Create or overwrite the file.
                   - 'a': Append data to an existing file.
        write_headers_or_rows (bool): Determines whether to write headers or data:
                                  - True: Write column headers.
                                  - False: Write data rows.
        data (list[dict]): Data to write to the CSV file. It should be a list of dictionaries,
                     where each dictionary represents a row, and the keys are the column names.
    """
    with open(file_direction, mode, newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELD_NAMES)
        if write_headers_or_rows:
            writer.writeheader()
        else:
            writer.writerows(data)
            
def get_number_tabs(url: str) -> int:
    """Returns the number of tabs (pages) available on the website for restaurant listings.

    Args:
        url (str): The base URL of the website.

    Returns:
        int: The total number of tabs (pages).
    """
    html_restaurants_list = get_html_content(url+"en/mx/restaurants/page/1")
    component_html = scrape_html_component(html_restaurants_list,"find","div","search-results__column col-lg-12")
    num_pages = len(scrape_html_component(component_html,"find_all","li","")) + 1
    return num_pages

def extract_restaurants_links(url: str, number_page: int) -> list[str]:
    """Extracts a list of URLs for restaurants from a specific page of the website.

    Args:
        url (str): The base URL of the website.
        number_page (int): The page number (tab) of the restaurant list.

    Returns:
        list[str]: A list of URLs, each pointing to a restaurant's page.
    """
    html_restaurants_list = get_html_content(url+"en/mx/restaurants/page/"+str(number_page))
    component_restaurants_list = scrape_html_component(html_restaurants_list,"find","div","row restaurant__list-row js-restaurant__list_items")
    restaurants_links = extract_url_pages(scrape_html_component(component_restaurants_list,"find_all","a","link"))
    return restaurants_links

def get_name(html: BeautifulSoup) -> str:
    """Extracts and returns the name of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: The name of the restaurant.
    """
    name = scrape_html_component(html,"find","h1","data-sheet__title")
    return name.text.strip()

def get_food(html: BeautifulSoup) -> str:
    """Extracts and returns the food type(s) of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: A comma-separated string of the restaurant's food type(s).
    """
    food_type_component = scrape_html_component(html,"find_all","div","data-sheet__block--text")
    # Clean the string by removing unwanted characters and splitting by commas
    food_type = (
        food_type_component[1]
        .text
        .translate(str.maketrans("", "", "$· \n")) 
        .split(",") 
    )
    # Filter and standardize the data
    if len(food_type) > 1:
        if "Mexican" in food_type[1]:
            food_type.reverse()  
            
        food_type_mapping = {
            "Mexican": ["Mexican"] if food_type[1] == "TraditionalCuisine" else food_type,
            "Mexican": ["International"] if food_type[1] == "International" else food_type,
            "Italian": ["Italian"]
        }
        food_type = food_type_mapping.get(food_type[0], food_type) 
    return ', '.join(food_type)

def get_country_zipcode(html: BeautifulSoup) -> str:
    """Extracts and returns the country and zip code of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        tuple[str, str]: A tuple containing the country and zip code of the restaurant.
    """
    country_zip_comp = scrape_html_component(html,"find_all","div","data-sheet__block--text")
    country_zip = (country_zip_comp[0].text.strip()).split(',')
    return country_zip[len(country_zip)-1], country_zip[len(country_zip)-2]

def get_state(html: BeautifulSoup) -> str:
    """Extracts and returns the state of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: The state where the restaurant is located.
    """
    state = scrape_html_component(html,"find_all","li","breadcrumb-item")[2]
    return state.text.strip()

def get_clasification(html: BeautifulSoup) -> str:
    """Extracts and returns the distinction and sustainability classification of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        tuple[str, str]: A tuple containing the distinction and sustainability classification of the restaurant.
                        Defaults to ("-", "-") if no classification is found.
    """
    global DISTINCTION_NAMES
    distinction: str = '-'
    sustainability: str = "-"
    clasification_component = scrape_html_component(html,"find_all","div","data-sheet__classification-item--content")
    
    if len(clasification_component) > 0:
        for name in DISTINCTION_NAMES[:3]: 
            if name in str(clasification_component):
                distinction = name
                break
        if DISTINCTION_NAMES[3] in str(clasification_component):
            sustainability = DISTINCTION_NAMES[3]
    return distinction, sustainability      
                                       
def create_and_export_restaurant_csv(restaurants_html: list[BeautifulSoup]) -> None:
    """Creates a list of dictionaries containing restaurant data and saves it to a CSV file.

    Args:
        restaurants_html (list[BeautifulSoup]): A list of BeautifulSoup objects, each representing
                                               the parsed HTML of a restaurant's webpage.
    """
    global RESTAURANT_ID
    global CSV_DIRECTION
    restaurants_data:list[dict[str]] = []
    for html in restaurants_html: 
        restaurants_data.append(
            {
                "id": str(RESTAURANT_ID),
                "name": get_name(html),
                "types of restaurants": get_food(html),
                "country": get_country_zipcode(html)[0],
                "state": get_state(html),
                "zip code": get_country_zipcode(html)[1],
                "distinction": get_clasification(html)[0],
                "sustainability":get_clasification(html)[1]
            }
        )
        RESTAURANT_ID= RESTAURANT_ID + 1
    write_csv(CSV_DIRECTION,"a",False,restaurants_data)

#-----------------------------------------------FUNCTIONS END---------------------------------------------------

#-----------------------------------------------ASYNC FUNCTIONS BEGING---------------------------------------------------
async def httpRequest(session: aiohttp.ClientSession ,url: str) -> BeautifulSoup:
    """Sends an asynchronous HTTP GET request to a URL and returns the parsed HTML as a BeautifulSoup object.

    Args:
        session (aiohttp.ClientSession): An aiohttp client session for handling multiple HTTP requests.
        url (str): The URL of the webpage to request.

    Returns:
        BeautifulSoup: The parsed HTML content of the webpage.
    """
    async with session.get(url) as response:
        return await response.text()

async def scrape_pages(url: str, number_pages: int) -> None:
    """Scrapes multiple pages of a website asynchronously, extracting restaurant data and saving it to a CSV.

    Args:
        BASE_URL (str): Base URL of the website to scrape (e.g., "https://example.com").
        number_pages (int): Total number of pages/tabs to scrape.

    Process Flow:
        1. Iterates through each page (from 1 to `number_pages - 1`).
        2. Fetches restaurant links for each page.
        3. Creates asynchronous HTTP requests for all links on the page.
        4. Parses HTML responses and saves data to a CSV file incrementally.
    """
    for resturant_page in range(1,number_pages):
        restaurants_links = extract_restaurants_links(url,resturant_page)
        async with aiohttp.ClientSession() as session:
            tasks_restaurants: list[asyncio.Task] = []
            async with asyncio.TaskGroup() as tg:
                for restaurant_URL in restaurants_links:
                    task = tg.create_task(httpRequest(session, (url + restaurant_URL)))
                    tasks_restaurants.append(task)
                
            restaurants_http = [task.result() for task in tasks_restaurants]
            restaurants_html = [BeautifulSoup(http,"html.parser") for http in restaurants_http]
            create_and_export_restaurant_csv(restaurants_html)
           
#-----------------------------------------------ASYNC FUNCTIONS END---------------------------------------------------
def main() -> None:
    """ Main function that orchestrates the web scraping workflow:
        1. Initializes the CSV file with headers.
        2. Determines the total number of pages to scrape.
        3. Executes asynchronous scraping of all pages.
        4. Prints completion message.

        Process Flow:
        - Creates/overwrites CSV file and writes column headers
        - Calculates total restaurant listing pages
        - Runs asynchronous scraping of all pages
    """
    write_csv(CSV_DIRECTION,"w",True,None)
    NUMBER_PAGES = get_number_tabs(BASE_URL)
    asyncio.run(scrape_pages(BASE_URL, NUMBER_PAGES))
    print("done")

if __name__ == '__main__':
    main()