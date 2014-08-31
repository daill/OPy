from html.parser import HTMLParser
import re
import urllib.request
import sys

__author__ = 'daill'


class ProtocolParser(HTMLParser):
  def __init__(self):
    super().__init__()

    self.profile_name = ''
    self.tag_stack = list()
    self.active_tag = None
    self.profile_found = False
    self.special_case = False
    self.profile_pattern = re.compile('Request:(.*)Response:(.*)', re.DOTALL)
    self.string_buffer = ''
    self.reset_profile()


  def handle_starttag(self, tag, attrs):
    def attrs_containing(name, value):
      for _name, _value  in attrs:
        if str(_name) == name and str(_value) == value:
          return True
        else:
          return False

    if tag == 'div' and attrs_containing("class", "highlight highlight-xml"):
      self.special_case = True


    self.tag_stack.append(tag)
    self.active_tag = tag


  def handle_endtag(self, tag):
    del self.tag_stack[len(self.tag_stack)-1]
    self.active_tag = self.tag_stack[len(self.tag_stack)-1]

    if self.special_case and tag == 'div':
      self.special_case = False
      self.extract_profiles(self.string_buffer)

  def extract_profiles(self, data):
    m = self.profile_pattern.match(data)
    if m != None:
      # print(self.tag_stack)
      self.profile_request = m.group(1).strip()
      self.profile_response = m.group(2).strip()
      self.print_profile()
      self.reset_profile()
      self.profile_found = False

  def handle_data(self, data):
    # print(str(self.active_tag) + " " + data + " " + str(data.isupper()) + " " + ("" if self.active_tag == None else self.active_tag) + " " + str(self.tag_stack))
    data = data.strip(' \t\n\r')
    if self.profile_found == True:
      if self.active_tag == 'code':
        self.extract_profiles(data)
      elif (self.active_tag == 'pre' or self.active_tag == 'span') and self.special_case:
        self.string_buffer += data
    elif self.active_tag == 'h2' or self.active_tag == 'h3':
      if data.isupper():
        # print(self.active_tag + " " + data)
        self.profile_found = True
        self.profile_name = data
        # print(self.profile_name)

  def reset_profile(self):
    self.profile_name = ''
    self.profile_request = ''
    self.profile_response = ''

  def print_profile(self):
    # print(self.active_tag)
    print("{} = Call(\"{}\", \"{}\")".format(self.profile_name.lower(), self.profile_request if self.profile_request != "empty" else "", self.profile_response if self.profile_response != "empty" else ""))



def parse_html(url):
  try:
    # html = urllib.request.urlopen(url).read().decode('utf-8')
    html = open("/Users/daill/Downloads/binary.html", "r").read()
    parser = ProtocolParser()
    parser.feed(data=html)

  except Exception as err:
    print(sys.last_traceback)


if __name__ == '__main__':
  parse_html("https://github.com/orientechnologies/orientdb/wiki/Network-Binary-Protocol")
