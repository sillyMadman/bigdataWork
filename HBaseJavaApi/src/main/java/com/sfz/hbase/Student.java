package com.sfz.hbase;

import java.util.List;
import java.util.Map;

public class Student {
    String name;
    String studentId;
    String classNumber;
    String understanding;
    String programming;


    public Student(String name, String studentId, String classNumber, String understanding, String programming) {
        this.name = name;
        this.studentId = studentId;
        this.classNumber = classNumber;
        this.understanding = understanding;
        this.programming = programming;
    }

    public String getName() {
        return name;
    }

    public String getStudentId() {
        return studentId;
    }

    public String getClassNumber() {
        return classNumber;
    }

    public String getUnderstanding() {
        return understanding;
    }

    public String getProgramming() {
        return programming;
    }
}
