Plugin API reference
=====================

This page contains auto-generated reference documentation for all Flytekit Plugins.

.. toctree::
   :maxdepth: 2

   {% for page in pages %}
   {% if page.top_level_object and page.display %}
   {{ page.include_path }}
   {% endif %}
   {% endfor %}
