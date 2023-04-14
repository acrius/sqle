{% from "pagination_macros.sql" import render_pagination %}

select * from profiles
{{ render_pagination() }};
