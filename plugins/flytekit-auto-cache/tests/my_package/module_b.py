from module_c import third_helper
from my_dir import bs

def another_helper():
    print("Another helper")
    third_helper()
    html = "<p>Hello, world!</p>"
    soup = bs.BeautifulSoup(html, "html.parser")
    print(soup.p.text)
