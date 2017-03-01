SRC="TriangleFirstMapper.java TriangleFirstReducer.java TriangleFirstPass.java TriangleSecondMapper.java TriangleSecondReducer.java TriangleEnumerator.java"
OBJ="TriangleFirstMapper*.class TriangleFirstReducer*.class TriangleFirstPass*.class TriangleSecondMapper*.class TriangleSecondReducer*.class TriangleEnumerator*.class"

if [ -d "triangle_first_pass_output" ]; then
    rm -rf triangle_first_pass_output
fi
if [ -d "triangle_output" ]; then
    rm -rf triangle_output
fi
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main -Xlint:unchecked -Xdiags:verbose ${SRC} && \
    jar cf te.jar $OBJ && \
    /usr/local/hadoop/bin/hadoop jar te.jar TriangleEnumerator augmenter_output triangle_output
