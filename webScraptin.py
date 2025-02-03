import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests 
import csv 
import time


#-----------------------------------------------GLOBAL VARIABLES BEGIN---------------------------------------------------
number_pages: int = 0
restaurant_id: int = 1
#Constant variables
base_url: str = "https://guide.michelin.com/"
field_names: list[str] = [
    "id",
    "name",
    "types of restaurants",
    "country",
    "state",
    "zip code",
    "distinction",
    "sustainability",
]
distinction_names: list[str] = ["Bib Gourmand", "One Star", "Two Stars:", "Green Star"]
#-----------------------------------------------GLOBAL VARIABLES END---------------------------------------------------

#-----------------------------------------------FUNCTIONS BEGING---------------------------------------------------
def htmlContent(URL: str) -> BeautifulSoup:
    """This function returns the HTML content of a URL
    
    Args:
        URL (str): The URL of the website to scrape.

    Returns:
        BeautifulSoup: The parsed HTML structure of the website.
    """
    return BeautifulSoup(requests.get(URL).content, "html.parser")

def htlmScrapt(html_data: BeautifulSoup, find: str, component: str, classs_: str) -> BeautifulSoup:
    """This function scrapes a specific component of an HTML document with a specified class.

    Args:
        html_data (BeautifulSoup): The parsed HTML structure of the website.
        find (str): The method to use for finding elements ("find" to get the first occurrence, "find_all" to get all occurrences).
        component (str): The HTML tag (e.g., "div", "span", "p") to search for.
        classs_ (str): The class name of the component to search for.

    Returns:
        BeautifulSoup object or list[BeautifulSoup object]: The HTML content of the component(s) that match the criteria.
    """
    return getattr(html_data, find)(component, class_=classs_)

def getUrlPages(htlmScrapt: list[BeautifulSoup]) -> list[str]:
    """Extracts a list of URLs from a list of HTML components.

    Args:
        htlmScrapt (list[BeautifulSoup]): A list of BeautifulSoup objects representing the HTML components to scrape.

    Returns:
        list[str]: A list of URLs (strings) extracted from the "href" attribute of the provided HTML components.
    """
    return [links["href"] for links in htlmScrapt if "href" in links.attrs]

def writeCSV(file_direction: str, mode: str, write_headers_rows: bool, data: list[dict] ) -> None:  
    """Creates or appends restaurant data to a CSV file.

    Args:
        mode (str): File opening mode:
                   - 'w': Create or overwrite the file.
                   - 'a': Append data to an existing file.
        write_headers_rows (bool): Determines whether to write headers or data:
                                  - True: Write column headers.
                                  - False: Write data rows.
        data (list[dict]): Data to write to the CSV file. It should be a list of dictionaries,
                     where each dictionary represents a row, and the keys are the column names.
    """
    #Open the file in the specified mode
    with open(file_direction, mode, newline="") as csvfile:
        #Create a csv.DictWriter object to write data to the CSV file
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        # Write headers or data based on the value of write_headers_rows
        if write_headers_rows:
            writer.writeheader()
        else:
            writer.writerows(data)
            
def numberTabs(base_url: str) -> int:
    """Returns the number of tabs (pages) available on the website for restaurant listings.

    Args:
        base_url (str): The base URL of the website.

    Returns:
        int: The total number of tabs (pages).
    """
    # Parse the HTML of the first page of the restaurant list
    html_restaurants_list = htmlContent(base_url+"en/mx/restaurants/page/1")
    # Find the container with the list of restaurant tabs
    content_html = htlmScrapt(html_restaurants_list,"find","div","search-results__column col-lg-12")
    # Get the total number of pages by counting the list items and adding 1
    number_pages = len(htlmScrapt(content_html,"find_all","li","")) + 1
    return number_pages

def restaurantsLinks(base_url: str, restaurant_page: int) -> list[str]:
    """Extracts a list of URLs for restaurants from a specific page of the website.

    Args:
        base_url (str): The base URL of the website.
        restaurant_page (int): The page number (tab) of the restaurant list.

    Returns:
        list[str]: A list of URLs, each pointing to a restaurant's page.
    """
    #Parse the HTML of the list pafe of the restaurants
    html_restaurants_list = htmlContent(base_url+"en/mx/restaurants/page/"+str(restaurant_page))
    #Find the container with the list of restaurants
    content_restaurants_list = htlmScrapt(html_restaurants_list,"find","div","row restaurant__list-row js-restaurant__list_items")
    #Get all of the URL for each restaurant
    restaurants_links = getUrlPages(htlmScrapt(content_restaurants_list,"find_all","a","link"))
    return restaurants_links

def getRestaurantName(html: BeautifulSoup) -> str:
    """Extracts and returns the name of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: The name of the restaurant.
    """
    #Find the restaurant name using the specified HTML tag and class
    name = htlmScrapt(html,"find","h1","data-sheet__title")
    return name.text.strip()

def getRestaurantFood(html: BeautifulSoup) -> str:
    """Extracts and returns the food type(s) of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: A comma-separated string of the restaurant's food type(s).
    """
    #Find the restaurant food type usign the specified HTML tag and class 
    food_type_component = htlmScrapt(html,"find_all","div","data-sheet__block--text")
    # Clean the string by removing unwanted characters and splitting by commas
    food_type = (
        food_type_component[1]
        .text
        .translate(str.maketrans("", "", "$· \n")) # Remove "$", "·", spaces, and newlines
        .split(",")  # Split into a list of food types
    )
    # Filter and standardize the data
    if len(food_type) > 1:
        if "Mexican" in food_type[1]:
            food_type.reverse()  # Reverse the list if "Mexican" is in the second position
            
        # Map food types to standardized values  
        food_type_mapping = {
            "Mexican": ["Mexican"] if food_type[1] == "TraditionalCuisine" else food_type,
            "Mexican": ["International"] if food_type[1] == "International" else food_type,
            "Italian": ["Italian"]
        }
        food_type = food_type_mapping.get(food_type[0], food_type) 
    # Return the food types as a comma-separated string   
    return ', '.join(food_type)

def getRestaurantCountryZipcode(html: BeautifulSoup) -> str:
    """Extracts and returns the country and zip code of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        tuple[str, str]: A tuple containing the country and zip code of the restaurant.
    """
    #Find the restaurant's location information using the specified HTML tag and class
    country_component = htlmScrapt(html,"find_all","div","data-sheet__block--text")
    #Clean and split the text to extract country and zip code
    country = (country_component[0].text.strip()).split(',')
    #Return the country (last element) and zip code (second-to-last element)
    return country[len(country)-1], country[len(country)-2]

def getRestaurantState(html: BeautifulSoup) -> str:
    """Extracts and returns the state of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        str: The state where the restaurant is located.
    """
    # Find the state information using the specified HTML tag and class
    state = htlmScrapt(html,"find_all","li","breadcrumb-item")[2]
    # Return the cleaned and stripped text of the state
    return state.text.strip()

def getRestaurantClasification(html: BeautifulSoup) -> str:
    """Extracts and returns the distinction and sustainability classification of the restaurant from the parsed HTML.

    Args:
        html (BeautifulSoup): The parsed HTML structure of the restaurant's webpage.

    Returns:
        tuple[str, str]: A tuple containing the distinction and sustainability classification of the restaurant.
                        Defaults to ("-", "-") if no classification is found.
    """
    # Initialize default values for distinction and sustainability
    distinction: str = '-'
    sustainability: str = "-"
    # Find the restaurant classification using the specified HTML tag and class
    clasification_content = htlmScrapt(html,"find_all","div","data-sheet__classification-item--content")
    
    # If classification data exists, filter it to extract distinction and sustainability
    if len(clasification_content) > 0:
        # Iterate through the first three distinction names
        for name in distinction_names[:3]: 
            # Check if the name is present in the classification content
            if name in str(clasification_content):
                distinction = name
                break
        # Check for sustainability separately
        if distinction_names[3] in str(clasification_content):
            sustainability = distinction_names[3]
    # Return the distinction and sustainability as a tuple
    return distinction, sustainability      
                                       
def saveInfo(restaurants_html: list[BeautifulSoup]) -> None:
    """Creates a list of dictionaries containing restaurant data and saves it to a CSV file.

    Args:
        restaurants_html (list[BeautifulSoup]): A list of BeautifulSoup objects, each representing
                                               the parsed HTML of a restaurant's webpage.
    """
    #Initialize the global variable for restaurant ID
    global restaurant_id
    restaurants_data:list[dict[str]] = []
    
    # Iterate through each restaurant's HTML
    for html in restaurants_html: 
        # Create a dictionary with the restaurant's data
        restaurants_data.append(
            {
                "id": str(restaurant_id),
                "name": getRestaurantName(html),
                "types of restaurants": getRestaurantFood(html),
                "country": getRestaurantCountryZipcode(html)[0],
                "state": getRestaurantState(html),
                "zip code": getRestaurantCountryZipcode(html)[1],
                "distinction": getRestaurantClasification(html)[0],
                "sustainability":getRestaurantClasification(html)[1]
            }
        )
        # Increment the restaurant ID for the next entry
        restaurant_id= restaurant_id + 1
    # Save the collected data to a CSV file
    writeCSV("Web Scrapping/restaurants2.csv","a",False,restaurants_data)

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
    # Send an asynchronous GET request to the URL
    async with session.get(url) as response:
        # Return the HTML content of the response as text
        return await response.text()

async def scrape_pages(base_url: str, number_pages: int) -> None:
    """Scrapes multiple pages of a website asynchronously, extracting restaurant data and saving it to a CSV.

    Args:
        base_url (str): Base URL of the website to scrape (e.g., "https://example.com").
        number_pages (int): Total number of pages/tabs to scrape.

    Process Flow:
        1. Iterates through each page (from 1 to `number_pages - 1`).
        2. Fetches restaurant links for each page.
        3. Creates asynchronous HTTP requests for all links on the page.
        4. Parses HTML responses and saves data to a CSV file incrementally.
    """
    # Iterate through all pages
    for resturant_page in range(1,number_pages):
        # Get restaurant URLs for the current page
        restaurants_links = restaurantsLinks(base_url,resturant_page)
        # Create an async HTTP session
        async with aiohttp.ClientSession() as session:
            #Initialize task list for concurrent requests
            tasks_restaurants: list[asyncio.Task] = []
            # Create a task group for parallel execution
            async with asyncio.TaskGroup() as tg:
                # Create async tasks for each restaurant URL
                for restaurant_URL in restaurants_links:
                    task = tg.create_task(httpRequest(session, (base_url + restaurant_URL)))
                    tasks_restaurants.append(task)
                
            # Extract HTML responses from completed tasks
            restaurants_http = [task.result() for task in tasks_restaurants]
            # Parse HTML content into BeautifulSoup objects
            restaurants_html = [BeautifulSoup(http,"html.parser") for http in restaurants_http]
            # Save extracted data to CSV
            saveInfo(restaurants_html)
           
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
    # Initialize CSV file with headers
    writeCSV("Web Scrapping/restaurants2.csv","w",True,None)
    # Get total number of pages to scrape
    number_pages = numberTabs(base_url)
    # Execute asynchronous scraping of all pages
    asyncio.run(scrape_pages(base_url, number_pages))
     # Confirm completion
    print("done")

if __name__ == '__main__':
    main()