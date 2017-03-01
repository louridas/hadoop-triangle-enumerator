SRC="EdgeAugmenter.java EdgeMapper.java EdgeReducer.java IdentityEdgeMapper.java PartialDegreeReducer.java"
OBJ="EdgeAugmenter*.class EdgeMapper*.class EdgeReducer*.class IdentityEdgeMapper*.class PartialDegreeReducer*.class"

if [ -d "augmenter_output" ]; then
    rm -rf augmenter_output
fi
if [ -d "step1_augmenter_output" ]; then
    rm -rf step1_augmenter_output
fi
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main -Xlint:unchecked -Xdiags:verbose ${SRC} && \
    jar cf ea.jar $OBJ && \
    /usr/local/hadoop/bin/hadoop jar ea.jar EdgeAugmenter input augmenter_output
