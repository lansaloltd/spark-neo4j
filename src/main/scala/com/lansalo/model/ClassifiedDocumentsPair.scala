package com.lansalo.model

final case class ClassifiedDocumentsPair(candidatePair: (Document, Document), confidenceLevel: Double, prediction: Boolean)
