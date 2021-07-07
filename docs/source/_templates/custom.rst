{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

{% if objtype == 'class' %}

.. autoclass:: {{ objname }}

   {% block methods %}
   {% if methods %}
   
   .. rubric:: {{ _('Methods') }}
   {% for item in methods %}
   .. automethod:: {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}

   .. rubric:: {{ _('Attributes') }}
   {% for item in attributes %}
   .. autoattribute:: {{ item }}
   {%- endfor %}

   {% endif %}
   {% endblock %}


{% endif %}

{% if objtype == 'function' %}

.. autofunction:: {{ objname }}

{% endif %}
