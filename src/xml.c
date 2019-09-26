/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2011 YAMAMOTO Naoki
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_LIBXML2
#include <libxml/parser.h>

#define API_INTERNAL
#include "nestalib.h"

APIEXPORT xmlDocPtr xml_open_doc(const char* xml, int length)
{
    xmlDocPtr doc;

    doc = xmlReadMemory(xml, length, "noname.xml", NULL, 0);
    if (doc == NULL) {
        err_write("xml: Failed to parse document");
        return NULL;
    }
    return doc;
}

APIEXPORT void xml_close_doc(xmlDocPtr doc)
{
    xmlFreeDoc(doc);
    xmlCleanupParser();
}

APIEXPORT xmlNode* xml_get_root(xmlDocPtr doc)
{
    xmlNode *node;

    node = xmlDocGetRootElement(doc);
    return node;
}

APIEXPORT xmlNode* xml_get_child(xmlNode* node, const char* tag)
{
    xmlNode* child;

    for (child = node->children; child != NULL; child = child->next) {
        if (child->type == XML_ELEMENT_NODE) {
            if (! strcmp((char *)child->name, tag))
                return child;
        }
    }
    return NULL;
}
#endif /* HAVE_LIBXML2 */
