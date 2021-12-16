package edu.montana.csci.csci440.helpers;

import edu.montana.csci.csci440.model.Employee;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EmployeeHelper {

    public static String makeEmployeeTree() {
        // TODO, change this to use a single query operation to get all employees
        Employee employee = Employee.find(1); // root employee
        // and use this data structure to maintain reference information needed to build the tree structure

        Map<Long, List<Employee>> reportsMap = new HashMap<>();

        for (Employee curEmployee : Employee.all()) {
            long reportsTo = curEmployee.getReportsTo();
            List<Employee> employeeReportsList = reportsMap.get(reportsTo);
            if (employeeReportsList == null) {
                employeeReportsList = new LinkedList<>();
                reportsMap.put(reportsTo, employeeReportsList);
            }
            employeeReportsList.add(curEmployee);
        }
        return "<ul>" + makeTree(employee, reportsMap)+ "<ul>";
    }


    // TODO - currently this method just uses the employee.getReports() function, which
    //  issues a query.  Change that to use the employeeMap variable instead
    public static String makeTree(Employee employee, Map<Long, List<Employee>> employeeMap) {
        String list = "<li><a href='/employees" + employee.getEmployeeId() + "'>"
                + employee.getEmail() + "</a><ul>";
        List<Employee> reports = employeeMap.get(employee.getEmployeeId());
        if(reports == null) {
            reports = new LinkedList<>();
        }
        for (Employee report : reports) {
            list += makeTree(report, employeeMap);
        }
        return list + "</ul></li>";
    }
}
