select i.number as 'Incident', 
s.label as 'State', 
ag.name as 'Assignment group', 
u.email as 'Assigned to' 
from incident as i
left join sys_user_group as ag on i.assignment_group = ag.sys_id
left join sys_user as u on i.assigned_to = u.sys_id
inner join sys_choice as s on i.state = s.value and s.name = 'incident' and s.element = 'state';

select p.number as 'Problem', 
s.label as 'State', 
ag.name as 'Assignment group', 
u.email as 'Assigned to' 
from problem as p
left join sys_user_group as ag on p.assignment_group = ag.sys_id
left join sys_user as u on p.assigned_to = u.sys_id
inner join sys_choice as s on p.state = s.value and s.name = 'problem' and s.element = 'state';

-- https://dev12604.service-now.com/api/now/table/sys_choice?sysparm_query=name%3Dincident%5EORname%3Dproblem&sysparm_display_value=true&sysparm_fields=sys_id%2Cname%2Celement%2Clabel%2Cvalue