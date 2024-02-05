"""
Module for dealing with data in XML format for data migrations and conversions.

This is designed primarily with XML documents with a structure similar to the following in mind. It
can be used to parse most XML documents, but if your document uses a more complex structure you may
find it beneficial to make your own class, using either `EtreeNodeSourceMixin` or 
`EtreeNodePipelineItemMixin` to still get some of the benefits of this module.

```xml
<root>
    <record id='1'>
        <somedata>Thing 1</somedata>
        <otherdata>We've got things.</otherdata>
    </record>
    <record id='2'>
        <somedata>Thing 2</somedata>
        <otherdata>Yep, all sorts of things.</otherdata>
    </record>
</root>
```

```python
source = XmlDocumentSource('records.xml', 'record')

source.take_attr('id') >> sink.put('id')
source.take('somedata') >> sink.put('some_data')
source.take('otherdata') >> sink.put('other_data')
```

XML as an output format is planned but not currently implemented.
"""
from ..pipeline.base import OnFail, Source, PipelineItem, Take
from xml.etree.ElementTree import Element, iterparse, tostring, XML, SubElement
from typing import Union
from io import IOBase
    
class EtreeNodeBaseMixin:
    namespaces = None
    def get(self, full_node=False):
        raise NotImplementedError('`get` must be overridden')
    
    def take(self, key, on_not_found=OnFail.fail) -> Source:
        """
        Take a child node using an xpath selector or tag name.

        This will pass the *text* of the found node to the next pipeline item, unless that node
        requests the full node. This allows using the result with normal pipeline items, but still
        allows multiple takes to be chained to get at child nodes.
        """
        return self >> EtreeTake(key, on_not_found, namespaces=self.namespaces)
    
    def take_attr(self, key, on_not_found=OnFail.fail) -> Source:
        """
        Take the value of an XML attribute from the node.
        """
        return self >> EtreeTakeAttr(key, on_not_found)

    # def take_multi(self, key, on_not_found=OnFail.fail) -> Source:
    #     return super().take(key, on_not_found)
    
    # def take_as_node(self, key, on_not_found=OnFail.fail) -> Source:
    #     return super().take(key, on_not_found)
    
    # def take_multi_as_nodes(self, key, on_not_found=OnFail.fail) -> Source:
    #     return super().take(key, on_not_found)


class EtreeNodeSourceMixin(EtreeNodeBaseMixin):
    def get(self, full_node=False):
        return super().get()


class EtreeNodePipelineItemMixin(EtreeNodeBaseMixin):
    def get(self, full_node=False):
        if not self._is_cached:
            if isinstance(self._prev, EtreeNodeBaseMixin):
                self._value = self.process(self._prev.guarded_get(full_node=full_node))
            else:
                self._value = self.process(self._prev.guarded_get())
            self._is_cached = True
        if full_node:
            return self._value
        else:
            return self._value.text
        
    def process(self, value) -> Element:
        raise NotImplementedError('PipelineItem.process must be overridden')


class EtreeTake(EtreeNodePipelineItemMixin, Take):
    def __init__(self, key, on_not_found=OnFail.fail, namespaces=None):
        self.key = key
        self.on_not_found = on_not_found
        self.namespaces = namespaces
    
    def get(self, full_node=False):
        parent_node = self._prev.guarded_get(full_node=True)
        if parent_node is None:
            return None
        if full_node:
            node = parent_node.find(self.key, self.namespaces)
        else:
            node = parent_node.findtext(self.key, None, self.namespaces)
        if node is None:
            self.on_not_found(KeyError())
        return node
        

# class EtreeTakeMulti(EtreeNodePipelineItemMixin, PipelineItem):
#     pass


class EtreeTakeAttr(Take):
    def get(self):
        parent_node = self._prev.guarded_get(full_node=True)
        if parent_node is None:
            return None
        attr = parent_node.get(self.key)
        if attr is None:
            self.on_not_found(KeyError())
        return attr


class ElementSource(EtreeNodeSourceMixin, Source):
    """
    A source that receives an already-parsed `ElementTree.Element` object and iterates over child 
    nodes matching the given xPath string as rows.
    """
    def __init__(self, node:Element, xpath:str, namespaces=None):
        self.node = node
        self.xpath = xpath
        self.namespaces = namespaces
        self._iter = iter(node.iterfind(xpath, namespaces))

    def get(self, full_node=False):
        if full_node:
            return self._value
        else:
            return self._value.text
            
    def next(self):
        self._value = next(self._iter) # Deliberately allow StopIteration to propagate


class XmlDocumentSource(EtreeNodeSourceMixin, Source):
    """
    A source that receives an unparsed XML file, and uses incremental parsing to return specific 
    elements.

    This discards elements after processing them, meaning it should be better than `ElementSource` 
    for large files. However, the downside is you can only match elements based on tag, not with a 
    full xpath name.
    """
    def __init__(self, xml_file, tag, parser=None, namespaces=None):
        self.namespaces = namespaces
        self.tag = tag
        self._iter = iter(iterparse(xml_file, parser=parser))
        self._value = None
    
    def get(self):
        return self._value
            
    def next(self):
        if self._value:
            # Delete elements as we go to save memory
            # FIXME this deletes the element contents, but not the element itself.
            # Could be a problem for *extremely* large documents, but not in most cases.
            # See https://web.archive.org/web/20201111201837/http://effbot.org/zone/element-iterparse.htm#incremental-parsing
            self._value.clear()
        while True:
            # Find the next element matching the tag
            event, self._value = next(self._iter) # Deliberately allow StopIteration to propagate
            if event == 'end' and self._value.tag == self.tag:
                return
    

class ParseXml(EtreeNodePipelineItemMixin, PipelineItem):
    """
    Receives a string containing XML data, allowing data to be extracted from it.

    Example::

        with source.take('xml_data') >> ParseXml() as xml:
            xml.take_attr('id') >> sink.put('id')
            xml.take('childtag1') >> sink.put('thing1')
            xml.take('childtag2') >> sink.put('thing2')
       
    Useful in conjunction with `FilesSource`::

        source = FilesSource('*.xml') >> ParseXml()
    """
    def __init__(self, parser=None):
        self.parser = parser
    
    def process(self, value):
        return XML(value, self.parser)


# class CollectXmlNode():
#     pass


class XmlDocumentSink():
    """
    A sink which outputs rows as elements in a single large XML document
    """
    def __init__(self, xml_file, root_element:Union[Element,str], row_element_name:str, container_element:Union[Element,str]=None):
        """
        :param xml_file: The file to output to
        :param root_element: The root element of the document tree to output. Can also be a string,
            which will be used to construct an Element object.
        :param row_element_name: The tag name for the elements that will be created to represent 
            each data row
        :param container_element: If the element to populate with records differs from the root 
            element, specify it here. It can either be an Element object (which must be a child of 
            root_element) or an xpath selector to select the value from the root element.
        """
        self.xml_file = xml_file
        self.row_element_name = row_element_name
        self._tail = None
        if not isinstance(root_element, Element):
            root_element = Element(root_element)
        self.root_element = root_element
        if container_element is None:
            self.container_element = root_element
        elif isinstance(container_element, Element):
            self.container_element = container_element
        else:
            self.container_element = root_element.find(container_element)

    def open(self):
        if not isinstance(self.xml_file, IOBase):
            self.xml_file = open(self.xml_file, 'w')
        # Create a fake element we can use as a placeholder to build the XML file
        el = SubElement(self.container_element, '__micdrop_xml_content_placeholder_element__')
        template = tostring(self.root_element, 'unicode', xml_declaration=True)
        head, tail = template.split(tostring(el, 'unicode'))
        self.xml_file.write(head)
        self._tail = tail
        
    def get(self):
        # TODO support putting attributes as well as children
        # TODO support putting Element objects directly
        children = super().get()
        el = Element(self.row_element_name)
        for key, value in children.items():
            sub = SubElement(el, key)
            sub.text = str(value)
        self.xml_file.write(tostring(el, 'unicode'))
    
    def close(self):
        if isinstance(self.xml_file, IOBase):
            self.xml_file.write(self._tail)
            self.xml_file.truncate()
            self.xml_file.close()
        