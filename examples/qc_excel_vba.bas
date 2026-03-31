Attribute VB_Name = "QCClimate"
Option Explicit

' ============================================================================
' Excel VBA Daily Climate QC
' ----------------------------------------------------------------------------
' Expected input sheet: "Data"
' Expected headers (row 1):
' Year, Month, Day, and variable columns such as:
' Tx, Tn, rr, w, dd, sd, fs, sc
'
' Output sheet: "QC_Flags"
' Columns: Var, Year, Month, Day, Hour, Minute, Value, Test
' ============================================================================

Private Const SHEET_DATA As String = "Data"
Private Const SHEET_FLAGS As String = "QC_Flags"

' Daily out-of-range thresholds
Private Const TMAX_UPPER As Double = 45
Private Const TMAX_LOWER As Double = -30
Private Const TMIN_UPPER As Double = 30
Private Const TMIN_LOWER As Double = -40
Private Const RR_UPPER As Double = 200
Private Const RR_LOWER As Double = 0
Private Const W_UPPER As Double = 30
Private Const W_LOWER As Double = 0
Private Const DD_UPPER As Double = 360
Private Const DD_LOWER As Double = 0
Private Const SC_UPPER As Double = 100
Private Const SC_LOWER As Double = 0
Private Const SD_UPPER As Double = 200
Private Const SD_LOWER As Double = 0
Private Const FS_UPPER As Double = 100
Private Const FS_LOWER As Double = 0

' Temporal coherence jumps
Private Const TEMP_JUMP As Double = 20
Private Const WIND_JUMP As Double = 15
Private Const SNOW_JUMP As Double = 50

' Repetition minimum run length
Private Const REP_N As Long = 4

Public Sub RunQC_Daily()
    Dim wsData As Worksheet, wsFlags As Worksheet
    Dim headers As Object
    Dim lastRow As Long, lastCol As Long
    Dim dataArr As Variant

    Set wsData = ThisWorkbook.Worksheets(SHEET_DATA)
    Set wsFlags = GetOrCreateSheet(SHEET_FLAGS)

    lastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column

    If lastRow < 2 Then
        MsgBox "No data rows found in sheet '" & SHEET_DATA & "'.", vbExclamation
        Exit Sub
    End If

    Set headers = HeaderMap(wsData, lastCol)
    ValidateRequiredHeaders headers

    dataArr = wsData.Range(wsData.Cells(1, 1), wsData.Cells(lastRow, lastCol)).Value2

    InitFlagSheet wsFlags

    ' Main tests
    TestDuplicateDates dataArr, headers, wsFlags
    TestDailyOutOfRange dataArr, headers, wsFlags
    TestDailyRepetition dataArr, headers, wsFlags
    TestTemporalCoherence dataArr, headers, wsFlags
    TestInternalConsistency dataArr, headers, wsFlags
    TestClimaticOutliers dataArr, headers, wsFlags

    MsgBox "QC completed. Flags written to sheet '" & SHEET_FLAGS & "'.", vbInformation
End Sub

' -------------------------
' Test: Duplicate dates
' -------------------------
Private Sub TestDuplicateDates(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim r As Long
    Dim key As String
    Dim d As Object
    Dim y As Variant, m As Variant, dday As Variant

    Set d = CreateObject("Scripting.Dictionary")

    For r = 2 To UBound(arr, 1)
        y = arr(r, headers("year"))
        m = arr(r, headers("month"))
        dday = arr(r, headers("day"))

        If IsNumeric(y) And IsNumeric(m) And IsNumeric(dday) Then
            key = CStr(CLng(y)) & "-" & CStr(CLng(m)) & "-" & CStr(CLng(dday))
            If d.Exists(key) Then
                AddDateRowFlags arr, headers, wsFlags, d(key), "duplicate_dates"
                AddDateRowFlags arr, headers, wsFlags, r, "duplicate_dates"
            Else
                d.Add key, r
            End If
        End If
    Next r
End Sub

' -------------------------
' Test: Daily out-of-range
' -------------------------
Private Sub TestDailyOutOfRange(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim vars As Variant
    Dim i As Long, r As Long
    Dim v As String
    Dim lower As Double, upper As Double
    Dim value As Variant

    vars = Array("Tx", "Tn", "rr", "w", "dd", "sc", "sd", "fs")

    For i = LBound(vars) To UBound(vars)
        v = CStr(vars(i))
        If headers.Exists(LCase$(v)) Then
            GetRangeThresholds v, lower, upper
            For r = 2 To UBound(arr, 1)
                value = arr(r, headers(LCase$(v)))
                If IsNumeric(value) Then
                    If CDbl(value) < lower Or CDbl(value) > upper Then
                        AddFlag wsFlags, v, arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", value, "daily_out_of_range"
                    End If
                End If
            Next r
        End If
    Next i
End Sub

' -------------------------
' Test: Daily repetition
' -------------------------
Private Sub TestDailyRepetition(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim vars As Variant
    Dim i As Long

    vars = Array("Tx", "Tn", "rr", "w", "dd", "sc", "sd", "fs")

    For i = LBound(vars) To UBound(vars)
        If headers.Exists(LCase$(CStr(vars(i)))) Then
            DetectRuns arr, headers, wsFlags, CStr(vars(i)), REP_N, "daily_repetition"
        End If
    Next i
End Sub

' -------------------------
' Test: Temporal coherence
' -------------------------
Private Sub TestTemporalCoherence(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim vars As Variant
    Dim i As Long, r As Long
    Dim v As String
    Dim jumpMax As Double
    Dim currVal As Variant, prevVal As Variant
    Dim currDate As Double, prevDate As Double

    vars = Array("Tx", "Tn", "w", "sd")

    For i = LBound(vars) To UBound(vars)
        v = CStr(vars(i))
        If headers.Exists(LCase$(v)) Then
            If v = "Tx" Or v = "Tn" Then
                jumpMax = TEMP_JUMP
            ElseIf v = "w" Then
                jumpMax = WIND_JUMP
            Else
                jumpMax = SNOW_JUMP
            End If

            For r = 3 To UBound(arr, 1)
                currVal = arr(r, headers(LCase$(v)))
                prevVal = arr(r - 1, headers(LCase$(v)))

                If IsNumeric(currVal) And IsNumeric(prevVal) Then
                    currDate = ToDateSerial(arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")))
                    prevDate = ToDateSerial(arr(r - 1, headers("year")), arr(r - 1, headers("month")), arr(r - 1, headers("day")))

                    If currDate - prevDate = 1 Then
                        If Abs(CDbl(currVal) - CDbl(prevVal)) > jumpMax Then
                            AddFlag wsFlags, v, arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", currVal, "temporal_coherence"
                            AddFlag wsFlags, v, arr(r - 1, headers("year")), arr(r - 1, headers("month")), arr(r - 1, headers("day")), "", "", prevVal, "temporal_coherence"
                        End If
                    End If
                End If
            Next r
        End If
    Next i
End Sub

' -------------------------
' Test: Internal consistency
' -------------------------
Private Sub TestInternalConsistency(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim r As Long
    Dim tx As Variant, tn As Variant
    Dim w As Variant, dd As Variant
    Dim sc As Variant, sd As Variant
    Dim fs As Variant
    Dim currDate As Double, prevDate As Double
    Dim sdDiff As Double

    For r = 2 To UBound(arr, 1)
        ' Tx / Tn
        If headers.Exists("tx") And headers.Exists("tn") Then
            tx = arr(r, headers("tx"))
            tn = arr(r, headers("tn"))
            If IsNumeric(tx) And IsNumeric(tn) Then
                If CDbl(tx) < CDbl(tn) Then
                    AddFlag wsFlags, "Tx", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", tx, "internal_consistency"
                    AddFlag wsFlags, "Tn", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", tn, "internal_consistency"
                End If
            End If
        End If

        ' w / dd
        If headers.Exists("w") And headers.Exists("dd") Then
            w = arr(r, headers("w"))
            dd = arr(r, headers("dd"))
            If IsNumeric(w) Then
                If CDbl(w) = 0 And Not IsEmpty(dd) And CStr(dd) <> "" Then
                    AddFlag wsFlags, "w", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", w, "internal_consistency"
                    AddFlag wsFlags, "dd", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", dd, "internal_consistency"
                End If
            End If
        End If

        ' sc / sd
        If headers.Exists("sc") And headers.Exists("sd") Then
            sc = arr(r, headers("sc"))
            sd = arr(r, headers("sd"))
            If IsNumeric(sc) And IsNumeric(sd) Then
                If CDbl(sc) = 0 And CDbl(sd) > 0 Then
                    AddFlag wsFlags, "sc", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", sc, "internal_consistency"
                    AddFlag wsFlags, "sd", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", sd, "internal_consistency"
                End If
            End If
        End If

        ' fs / Tn
        If headers.Exists("fs") And headers.Exists("tn") Then
            fs = arr(r, headers("fs"))
            tn = arr(r, headers("tn"))
            If IsNumeric(fs) And IsNumeric(tn) Then
                If CDbl(fs) > 0 And CDbl(tn) > 3 Then
                    AddFlag wsFlags, "fs", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", fs, "internal_consistency"
                    AddFlag wsFlags, "Tn", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", tn, "internal_consistency"
                End If
            End If
        End If

        ' fs / sd with day-to-day increase in sd and fs = 0
        If r > 2 And headers.Exists("fs") And headers.Exists("sd") Then
            If IsNumeric(arr(r, headers("sd"))) And IsNumeric(arr(r - 1, headers("sd"))) And IsNumeric(arr(r, headers("fs"))) Then
                currDate = ToDateSerial(arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")))
                prevDate = ToDateSerial(arr(r - 1, headers("year")), arr(r - 1, headers("month")), arr(r - 1, headers("day")))
                sdDiff = CDbl(arr(r, headers("sd"))) - CDbl(arr(r - 1, headers("sd")))

                If currDate - prevDate = 1 And sdDiff > 0 And CDbl(arr(r, headers("fs"))) = 0 Then
                    AddFlag wsFlags, "fs", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", arr(r, headers("fs")), "internal_consistency"
                    AddFlag wsFlags, "sd", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", arr(r, headers("sd")), "internal_consistency"
                End If
            End If
        End If

        ' sd / Tn with day-to-day increase in sd and Tn > 2.5
        If r > 2 And headers.Exists("sd") And headers.Exists("tn") Then
            If IsNumeric(arr(r, headers("sd"))) And IsNumeric(arr(r - 1, headers("sd"))) And IsNumeric(arr(r, headers("tn"))) Then
                currDate = ToDateSerial(arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")))
                prevDate = ToDateSerial(arr(r - 1, headers("year")), arr(r - 1, headers("month")), arr(r - 1, headers("day")))
                sdDiff = CDbl(arr(r, headers("sd"))) - CDbl(arr(r - 1, headers("sd")))

                If currDate - prevDate = 1 And sdDiff > 0 And CDbl(arr(r, headers("tn"))) > 2.5 Then
                    AddFlag wsFlags, "sd", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", arr(r, headers("sd")), "internal_consistency"
                    AddFlag wsFlags, "Tn", arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", arr(r, headers("tn")), "internal_consistency"
                End If
            End If
        End If
    Next r
End Sub

' -------------------------
' Test: Climatic outliers by month (IQR rule)
' -------------------------
Private Sub TestClimaticOutliers(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet)
    Dim vars As Variant
    Dim i As Long

    vars = Array("Tx", "Tn", "ta", "rr", "w", "dd", "sc", "sd", "fs")

    For i = LBound(vars) To UBound(vars)
        If headers.Exists(LCase$(CStr(vars(i)))) Then
            DetectMonthlyOutliers arr, headers, wsFlags, CStr(vars(i))
        End If
    Next i
End Sub

' ============================================================================
' Helpers
' ============================================================================

Private Sub DetectMonthlyOutliers(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet, ByVal varName As String)
    Dim outrange As Double
    Dim m As Long
    Dim vals As Variant
    Dim q1 As Double, q3 As Double, iqr As Double
    Dim lower As Double, upper As Double
    Dim r As Long, v As Variant
    Dim nonMissing As Long

    nonMissing = CountNumeric(arr, headers(LCase$(varName)))
    If nonMissing <= 1825 Then Exit Sub ' same spirit as R: at least 5 years daily

    If varName = "rr" Then
        outrange = 5
    ElseIf varName = "Tx" Or varName = "Tn" Or varName = "ta" Then
        outrange = 3
    Else
        outrange = 4
    End If

    For m = 1 To 12
        vals = MonthValues(arr, headers, varName, m)
        If IsEmpty(vals) Then GoTo NextMonth

        q1 = Application.WorksheetFunction.Quartile_Inc(vals, 1)
        q3 = Application.WorksheetFunction.Quartile_Inc(vals, 3)
        iqr = q3 - q1
        lower = q1 - outrange * iqr
        upper = q3 + outrange * iqr

        For r = 2 To UBound(arr, 1)
            If IsNumeric(arr(r, headers("month"))) Then
                If CLng(arr(r, headers("month"))) = m Then
                    v = arr(r, headers(LCase$(varName)))
                    If IsNumeric(v) Then
                        If IsBoundedVar(varName) And CDbl(v) = 0 Then GoTo NextRow
                        If CDbl(v) < lower Or CDbl(v) > upper Then
                            AddFlag wsFlags, varName, arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", v, "climatic_outliers"
                        End If
                    End If
                End If
            End If
NextRow:
        Next r
NextMonth:
    Next m
End Sub

Private Function MonthValues(ByRef arr As Variant, ByVal headers As Object, ByVal varName As String, ByVal monthValue As Long) As Variant
    Dim tmp() As Double
    Dim n As Long
    Dim r As Long
    Dim v As Variant

    ReDim tmp(1 To UBound(arr, 1))
    n = 0

    For r = 2 To UBound(arr, 1)
        If IsNumeric(arr(r, headers("month"))) Then
            If CLng(arr(r, headers("month"))) = monthValue Then
                v = arr(r, headers(LCase$(varName)))
                If IsNumeric(v) Then
                    If IsBoundedVar(varName) And CDbl(v) = 0 Then GoTo NextR
                    n = n + 1
                    tmp(n) = CDbl(v)
                End If
            End If
        End If
NextR:
    Next r

    If n = 0 Then
        MonthValues = Empty
    Else
        ReDim Preserve tmp(1 To n)
        MonthValues = tmp
    End If
End Function

Private Function CountNumeric(ByRef arr As Variant, ByVal colIdx As Long) As Long
    Dim r As Long
    Dim n As Long
    n = 0
    For r = 2 To UBound(arr, 1)
        If IsNumeric(arr(r, colIdx)) Then n = n + 1
    Next r
    CountNumeric = n
End Function

Private Sub DetectRuns(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet, ByVal varName As String, ByVal minRun As Long, ByVal testName As String)
    Dim colIdx As Long
    Dim r As Long
    Dim runStart As Long
    Dim runLen As Long
    Dim prevVal As Variant, currVal As Variant

    colIdx = headers(LCase$(varName))
    runStart = 2
    runLen = 1
    prevVal = arr(2, colIdx)

    For r = 3 To UBound(arr, 1)
        currVal = arr(r, colIdx)

        If ValuesEqual(prevVal, currVal) Then
            runLen = runLen + 1
        Else
            If runLen >= minRun Then
                EmitRunFlags arr, headers, wsFlags, varName, runStart, runLen, testName
            End If
            runStart = r
            runLen = 1
        End If

        prevVal = currVal
    Next r

    If runLen >= minRun Then
        EmitRunFlags arr, headers, wsFlags, varName, runStart, runLen, testName
    End If
End Sub

Private Sub EmitRunFlags(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet, ByVal varName As String, ByVal runStart As Long, ByVal runLen As Long, ByVal testName As String)
    Dim i As Long
    Dim r As Long
    Dim v As Variant

    For i = 0 To runLen - 1
        r = runStart + i
        v = arr(r, headers(LCase$(varName)))
        If IsBoundedVar(varName) And IsNumeric(v) Then
            If CDbl(v) = 0 Then GoTo NextI
        End If
        AddFlag wsFlags, varName, arr(r, headers("year")), arr(r, headers("month")), arr(r, headers("day")), "", "", v, testName
NextI:
    Next i
End Sub

Private Function ValuesEqual(ByVal a As Variant, ByVal b As Variant) As Boolean
    If IsNumeric(a) And IsNumeric(b) Then
        ValuesEqual = (CDbl(a) = CDbl(b))
    Else
        ValuesEqual = (CStr(a) = CStr(b))
    End If
End Function

Private Function IsBoundedVar(ByVal varName As String) As Boolean
    Select Case LCase$(varName)
        Case "rr", "sd", "fs", "sc", "sw"
            IsBoundedVar = True
        Case Else
            IsBoundedVar = False
    End Select
End Function

Private Sub AddDateRowFlags(ByRef arr As Variant, ByVal headers As Object, ByVal wsFlags As Worksheet, ByVal rowIdx As Long, ByVal testName As String)
    Dim vars As Variant
    Dim i As Long
    Dim v As String

    vars = Array("Tx", "Tn", "rr", "w", "dd", "sc", "sd", "fs")
    For i = LBound(vars) To UBound(vars)
        v = CStr(vars(i))
        If headers.Exists(LCase$(v)) Then
            AddFlag wsFlags, v, arr(rowIdx, headers("year")), arr(rowIdx, headers("month")), arr(rowIdx, headers("day")), "", "", arr(rowIdx, headers(LCase$(v))), testName
        End If
    Next i
End Sub

Private Sub GetRangeThresholds(ByVal varName As String, ByRef lower As Double, ByRef upper As Double)
    Select Case varName
        Case "Tx": lower = TMAX_LOWER: upper = TMAX_UPPER
        Case "Tn": lower = TMIN_LOWER: upper = TMIN_UPPER
        Case "rr": lower = RR_LOWER: upper = RR_UPPER
        Case "w": lower = W_LOWER: upper = W_UPPER
        Case "dd": lower = DD_LOWER: upper = DD_UPPER
        Case "sc": lower = SC_LOWER: upper = SC_UPPER
        Case "sd": lower = SD_LOWER: upper = SD_UPPER
        Case "fs": lower = FS_LOWER: upper = FS_UPPER
        Case Else: lower = -1E+99: upper = 1E+99
    End Select
End Sub

Private Sub AddFlag(ByVal ws As Worksheet, ByVal varName As String, ByVal y As Variant, ByVal m As Variant, ByVal d As Variant, ByVal h As Variant, ByVal minv As Variant, ByVal value As Variant, ByVal testName As String)
    Dim nextRow As Long
    nextRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row + 1

    ws.Cells(nextRow, 1).Value = varName
    ws.Cells(nextRow, 2).Value = y
    ws.Cells(nextRow, 3).Value = m
    ws.Cells(nextRow, 4).Value = d
    ws.Cells(nextRow, 5).Value = h
    ws.Cells(nextRow, 6).Value = minv
    ws.Cells(nextRow, 7).Value = value
    ws.Cells(nextRow, 8).Value = testName
End Sub

Private Function ToDateSerial(ByVal y As Variant, ByVal m As Variant, ByVal d As Variant) As Double
    If IsNumeric(y) And IsNumeric(m) And IsNumeric(d) Then
        ToDateSerial = CDbl(DateSerial(CLng(y), CLng(m), CLng(d)))
    Else
        ToDateSerial = 0
    End If
End Function

Private Sub InitFlagSheet(ByVal ws As Worksheet)
    ws.Cells.Clear
    ws.Range("A1:H1").Value = Array("Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test")
End Sub

Private Function GetOrCreateSheet(ByVal sheetName As String) As Worksheet
    On Error Resume Next
    Set GetOrCreateSheet = ThisWorkbook.Worksheets(sheetName)
    On Error GoTo 0

    If GetOrCreateSheet Is Nothing Then
        Set GetOrCreateSheet = ThisWorkbook.Worksheets.Add(After:=ThisWorkbook.Worksheets(ThisWorkbook.Worksheets.Count))
        GetOrCreateSheet.Name = sheetName
    End If
End Function

Private Function HeaderMap(ByVal ws As Worksheet, ByVal lastCol As Long) As Object
    Dim d As Object
    Dim c As Long
    Dim nameRaw As String

    Set d = CreateObject("Scripting.Dictionary")

    For c = 1 To lastCol
        nameRaw = Trim$(CStr(ws.Cells(1, c).Value))
        If Len(nameRaw) > 0 Then
            d(LCase$(nameRaw)) = c
        End If
    Next c

    Set HeaderMap = d
End Function

Private Sub ValidateRequiredHeaders(ByVal headers As Object)
    If Not headers.Exists("year") Then Err.Raise vbObjectError + 101, "RunQC_Daily", "Missing header: Year"
    If Not headers.Exists("month") Then Err.Raise vbObjectError + 102, "RunQC_Daily", "Missing header: Month"
    If Not headers.Exists("day") Then Err.Raise vbObjectError + 103, "RunQC_Daily", "Missing header: Day"
End Sub
