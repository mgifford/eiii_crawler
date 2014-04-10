""" Example code on how to get CSS values of a webpage
element using a remote selenium session """

# Requirement: python selenium
# $ sudo pip install selenium

from selenium import webdriver
from selenium.webdriver.common.by import By

def convert_to_hex(rgba_color):
    """ Convert rgba color to hex """
    # Color is in string
    print 'Rgba color is',rgba_color
    rgba = eval(rgba_color.replace('rgba',''))
    # Tuple now
    
    return '0x' + ''.join(map(lambda x: hex(int(x)).replace('0x',''),
                              rgba[:3]))

def do_test():
    driver = webdriver.Remote("http://tt5.s.tingtun.no:8910/wd/hub",
                              webdriver.DesiredCapabilities.FIREFOX.copy())

    driver.get("http://www.tingtun.no")
    # Locate the "Search" title
    title_elems = driver.find_elements(By.CLASS_NAME, "title")

    for item in title_elems:
        print 'Title =>',item.text

        if item.text == 'Search':
            # This is the element we want
            # Get CSS value of font-weight and color
            print '\tCSS font-weight =>',item.value_of_css_property('font-weight') # Prints 700 which means bold
            print '\tCSS color =>', convert_to_hex(item.value_of_css_property('color'))

    # Don't forget to do this otherwise the remote browser
    # window remains open and slowly eats memory of the server!
    driver.close()

if __name__ == "__main__":
    do_test()
