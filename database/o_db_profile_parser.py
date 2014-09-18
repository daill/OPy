from enum import Enum
import string
from database.o_db_constants import OTypes

__author__ = 'daill'


class OControl(Enum):
    """
    Enum to define control element within a string
    """
    LEFT_PARANTHESES = 1
    RIGHT_PARANTHESES = 2
    GROUP_LEFT = 3
    GROUP_RIGHT = 4
    OPTIONAL = 5
    MULTI = 6
    TEXT = 7
    GROUP_NAME_LEFT = 8
    GROUP_NAME_RIGHT = 9


class OKind(Enum):
    """
    Enum to difference between a term and a group
    """
    TERM = 1
    GROUP = 2


class OCondition(object):
    """
    This class builds a condition to decide in some case how the rest of the response
    will be handled
    """
    def __init__(self, condition_fn, condition_value):
        self.__fn = condition_fn
        self.__value = condition_value
        self.__result = False

    def eval(self, value):
        self.__result = self.__fn(self.__value, value)
        return self.__result

    def valid(self):
        return self.__result


class OElement(object):
    """
    Base class for all parts of a definition string
    """
    def __init__(self, kind: OKind):
        super().__init__()
        self.is_repeating = False
        self.kind = kind

    def __eq__(self, elem1):
        return self.kind == elem1.kind


class OTerm(OElement):
    """
    Represents a term like (value_name:type)
    """
    def __init__(self, profile: str):
        super().__init__(OKind.TERM)
        if not ":" in profile and not string.ascii_letters in profile:
            self.type = OTypes.BYTE_STATIC.value
            self.value = profile
            self.name = ''
        else:
            self.name, self.type = profile.split(":")


class OGroup(OElement):
    """
    This type of element holds a group of term which means it represents the [*] parts in the request/response
    profiles. Furthermore i've introduced {*} als name for the group. This is necessary to identify each group later
    in the parsed data dictionary
    """

    def __init__(self):
        super().__init__(OKind.GROUP)
        self.__member = list()
        self.name = ''

    def add_element(self, element: OElement):
        self.__member.append(element)

    def get_elements(self):
        return self.__member


class OProfile(object):
    def __init__(self):
        super().__init__()
        self.__elements = list()
        self.__element_index = 0

    def add_element(self, element: OElement):
        self.__elements.append(element)

    def get_elements(self):
        return self.__elements


class OProfileParser(object):
    """
    This class tries to parse the profile

    profile := ( term | group )*
    term ::= ( 'value_name:type' )*
    group ::= [ term | group ] '*' | '+'

    """

    def __init__(self):
        super().__init__()

        self.__index = 0
        self.__profile_str = ''
        self.__current_token = ''
        self.__current_text = ''
        self.__lookahead_token = ''

    def parse(self, profile_str=None):
        if profile_str != None:
            self.__profile_str = profile_str
            # do it twice to ensure that current_token isnt none
            self.read_token()
            self.read_token()

            return self.parse_profile()

    def parse_profile(self):
        profile = OProfile()

        while True:
            if self.__current_token == None:
                return profile
            elif self.__current_token == OControl.LEFT_PARANTHESES:
                # parse term
                element = self.parse_term()
            elif self.__current_token == OControl.GROUP_LEFT:
                # parse group
                element = self.parse_group()

            if element:
                profile.add_element(element)


    def parse_term(self):
        self.read_token()
        if self.__current_token == OControl.RIGHT_PARANTHESES:
            element = OTerm(self.__current_text)
            self.__current_text = ''

            if self.__lookahead_token == OControl.MULTI or self.__lookahead_token == OControl.OPTIONAL:
                element.is_repeating = True
                self.read_token()

            self.read_token()
            return element
        else:
            return None

    def parse_group(self):
        """
        Parses a group of elements indicated by [.
        :return:
        """
        self.read_token()
        element = OGroup()
        while self.__current_token != OControl.GROUP_RIGHT:
            if self.__current_token == OControl.GROUP_LEFT:
                element.add_element(self.parse_group())
            elif self.__current_token == OControl.LEFT_PARANTHESES:
                element.add_element(self.parse_term())
            elif self.__current_token == OControl.GROUP_NAME_RIGHT:
                element.name = self.__current_text
                self.__current_text = ''
                self.read_token()
            else:
                self.read_token()

        if self.__lookahead_token == OControl.MULTI or self.__lookahead_token == OControl.OPTIONAL:
            element.is_repeating = True
            self.read_token()

        self.read_token()
        return element

    # reads the next token either ( ) [ ] * + or just text
    def read_token(self):
        currentchar = self.next_char()
        self.__current_token = self.__lookahead_token

        while True:
            if currentchar == None:
                self.__lookahead_token = None
                return
            elif currentchar == '(':
                self.__lookahead_token = OControl.LEFT_PARANTHESES
                return
            elif currentchar == ')':
                self.__lookahead_token = OControl.RIGHT_PARANTHESES
                return
            elif currentchar == '[':
                self.__lookahead_token = OControl.GROUP_LEFT
                return
            elif currentchar == '{':
                self.__lookahead_token = OControl.GROUP_NAME_LEFT
                return
            elif currentchar == '}':
                self.__lookahead_token = OControl.GROUP_NAME_RIGHT
                return
            elif currentchar == ']':
                self.__lookahead_token = OControl.GROUP_RIGHT
                return
            elif currentchar == '+':
                self.__lookahead_token = OControl.MULTI
                return
            elif currentchar == '*':
                self.__lookahead_token = OControl.OPTIONAL
                return
            else:
                self.__lookahead_token = OControl.TEXT
                self.__current_text += currentchar

            currentchar = self.next_char()

    # reads the current char which should be processed
    def next_char(self):
        char = None
        if self.__index < len(self.__profile_str):
            char = self.__profile_str[self.__index]
            self.__index += 1

        return char