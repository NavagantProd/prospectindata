"""
analysis.py - Fixed Real Data B2B Prospecting Analysis with Separate Model Testing

Key fixes:
1. Fixed variable scoping issues
2. Improved target column detection
3. Fixed feature filtering logic
4. Added proper error handling
5. Fixed encoding issues for categorical variables
6. Improved data preprocessing pipeline
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
from pathlib import Path
from scipy import stats
from scipy.stats import shapiro, pearsonr, chi2_contingency
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from sklearn.feature_selection import SelectKBest, f_classif, chi2, mutual_info_classif
import warnings
warnings.filterwarnings('ignore')

# Create output directories
Path("static/charts").mkdir(parents=True, exist_ok=True)

def load_and_analyze_real_data(csv_path):
    """
    Load actual CSV data and analyze structure
    """
    print("="*60)
    print("1. LOADING REAL DATA")
    print("="*60)
    
    # Load data
    df = pd.read_csv(csv_path, low_memory=False)
    
    print(f"Dataset Shape: {df.shape}")
    print(f"Column Names: {list(df.columns)}")

    # Normalize column names
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    
    # Trim whitespace from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()

    # --- DROP IDENTIFIER FIELDS EARLY ---
    identifier_patterns = [
        '_id', 'name', 'email', 'contact_', '_url', 'timestamp', 'created_at', 'updated_at', 'date', 'time'
    ]
    id_cols = [c for c in df.columns if any(
        c.lower().endswith(p) or p in c.lower() for p in identifier_patterns
    )]
    if id_cols:
        print(f"Dropping identifier columns: {id_cols}")
        df = df.drop(columns=id_cols, errors='ignore')
    else:
        print("No identifier columns found to drop.")
    
    # Identify target variable
    target_candidates = ['email_opened', 'responded', 'converted', 'lead_status', 'response', 'open']
    target_col = None
    
    for candidate in target_candidates:
        if candidate in df.columns:
            target_col = candidate
            break
    
    # If no standard target found, look for binary columns
    if target_col is None:
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64'] and df[col].nunique() == 2:
                unique_vals = sorted(df[col].dropna().unique())
                if len(unique_vals) == 2 and set(unique_vals).issubset({0, 1, 0.0, 1.0}):
                    target_col = col
                    print(f"Found binary target: {target_col}")
                    break
    
    if target_col is None:
        print("No clear target variable found. Please specify target column.")
        print("Available columns:", list(df.columns))
        return None, None, None, None, None, None
    
    print(f"Target variable: {target_col}")
    
    # Convert target to binary if needed
    if df[target_col].dtype == 'object':
        # Try to convert to numeric
        unique_vals = df[target_col].dropna().unique()
        if len(unique_vals) == 2:
            le_target = LabelEncoder()
            df[target_col] = le_target.fit_transform(df[target_col].astype(str))
            print(f"Converted target to binary: {le_target.classes_}")
    
    # --- STRATIFIED 80/20 SPLIT ---
    min_test_size = max(20, int(0.2 * len(df)))
    try:
        train_df, test_df = train_test_split(
            df, test_size=min_test_size, stratify=df[target_col], random_state=42
        )
        print(f"Stratified 80/20 split: {train_df.shape[0]} train, {test_df.shape[0]} test")
        print(f"Train target distribution:\n{train_df[target_col].value_counts(normalize=True)}")
        print(f"Test target distribution:\n{test_df[target_col].value_counts(normalize=True)}")
    except Exception as e:
        print(f"Stratified split failed: {e}. Using random split.")
        train_df, test_df = train_test_split(df, test_size=min_test_size, random_state=42)
        print(f"Random 80/20 split: {train_df.shape[0]} train, {test_df.shape[0]} test")
    
    # Separate features into categorical and continuous
    categorical_cols = []
    continuous_cols = []
    
    for col in df.columns:
        if col != target_col:
            if df[col].dtype in ['object', 'category']:
                categorical_cols.append(col)
            elif df[col].dtype in ['int64', 'float64']:
                if df[col].nunique() > 10:  # Treat as continuous if more than 10 unique values
                    continuous_cols.append(col)
                else:
                    categorical_cols.append(col)  # Treat as categorical if few unique values
    
    print(f"\nCategorical columns ({len(categorical_cols)}): {categorical_cols}")
    print(f"Continuous columns ({len(continuous_cols)}): {continuous_cols}")
    
    return train_df, test_df, target_col, categorical_cols, continuous_cols, df

def preprocess_categorical_features(train_df, test_df, categorical_cols, target_col):
    """
    Preprocess categorical features for modeling
    """
    print("\n" + "="*60)
    print("2. PREPROCESSING CATEGORICAL FEATURES")
    print("="*60)
    
    train_processed = train_df.copy()
    test_processed = test_df.copy()
    categorical_features = []

    # 1. Drop leaky features
    leaky_patterns = [
        'view', 'opened', 'clicked', 'responded', 'post', 'status', 'error',
        'api_calls_made', 'distribution_id', 'enrichment_status', 'enrichment_error'
    ]
    leaky_features = [c for c in categorical_cols if any(p in c.lower() for p in leaky_patterns) and c != target_col]
    if leaky_features:
        print(f"Dropping leaky categorical features: {leaky_features}")
        train_processed = train_processed.drop(columns=leaky_features, errors='ignore')
        test_processed = test_processed.drop(columns=leaky_features, errors='ignore')
        categorical_cols = [c for c in categorical_cols if c not in leaky_features]

    import re
    # 2. Handle high-cardinality categoricals
    for col in list(categorical_cols):
        if col not in train_processed.columns:
            continue
            
        # Fill missing values first
        train_processed[col] = train_processed[col].fillna('Unknown')
        test_processed[col] = test_processed[col].fillna('Unknown')
        
        n_unique = train_processed[col].nunique()
        
        if ('url' in col.lower() or 'email' in col.lower()) and n_unique > 30:
            print(f"Domain bucketing for {col}")
            def extract_domain(val):
                if pd.isnull(val) or val == 'Unknown': 
                    return 'missing'
                m = re.search(r'([\w-]+\.[\w-]+)$', str(val))
                return m.group(1) if m else 'other'
            
            domain_col = col + '_domain'
            train_processed[domain_col] = train_processed[col].apply(extract_domain)
            test_processed[domain_col] = test_processed[col].apply(extract_domain)
            
            # Label encode the domain column
            le_domain = LabelEncoder()
            train_processed[domain_col + '_encoded'] = le_domain.fit_transform(train_processed[domain_col].astype(str))
            
            # Handle test set encoding
            test_values_domain = test_processed[domain_col].astype(str)
            test_encoded_domain = []
            for val in test_values_domain:
                if val in le_domain.classes_:
                    test_encoded_domain.append(le_domain.transform([val])[0])
                else:
                    test_encoded_domain.append(0)  # Unknown category
            test_processed[domain_col + '_encoded'] = test_encoded_domain
            
            categorical_features.append(domain_col + '_encoded')
            print(f"Label encoded domain column: {domain_col} (unique: {train_processed[domain_col].nunique()})")
            
            # Clean up
            train_processed = train_processed.drop(columns=[col, domain_col])
            test_processed = test_processed.drop(columns=[col, domain_col])
            
        elif n_unique > 30 and col != target_col:
            print(f"Target encoding for high-cardinality {col}")
            # Simple target encoding with smoothing
            global_mean = train_processed[target_col].mean()
            category_means = train_processed.groupby(col)[target_col].mean()
            category_counts = train_processed.groupby(col)[target_col].count()
            
            # Smooth with global mean
            smoothing = 10
            smoothed_means = (category_means * category_counts + global_mean * smoothing) / (category_counts + smoothing)
            
            train_processed[col + '_te'] = train_processed[col].map(smoothed_means)
            test_processed[col + '_te'] = test_processed[col].map(smoothed_means).fillna(global_mean)
            
            categorical_features.append(col + '_te')
            print(f"Target encoded {col}")
            
            # Clean up
            train_processed = train_processed.drop(columns=[col])
            test_processed = test_processed.drop(columns=[col])
            
        else:
            # Standard label encoding for other categorical variables
            le = LabelEncoder()
            train_processed[f'{col}_encoded'] = le.fit_transform(train_processed[col].astype(str))
            
            # Handle test set encoding
            test_values = test_processed[col].astype(str)
            test_encoded = []
            for val in test_values:
                if val in le.classes_:
                    test_encoded.append(le.transform([val])[0])
                else:
                    test_encoded.append(0)  # Unknown category
            test_processed[f'{col}_encoded'] = test_encoded
            
            categorical_features.append(f'{col}_encoded')
            print(f"Label encoded {col} ({n_unique} unique values)")

    # Final check for NaN values
    for col in categorical_features:
        if train_processed[col].isnull().any() or test_processed[col].isnull().any():
            fill_val = train_processed[col].mode().iloc[0] if not train_processed[col].mode().empty else 0
            train_processed[col] = train_processed[col].fillna(fill_val)
            test_processed[col] = test_processed[col].fillna(fill_val)
            print(f"Filled NaN values in {col} with {fill_val}")

    if categorical_features:
        X_train_cat = train_processed[categorical_features]
        X_test_cat = test_processed[categorical_features]
        y_train = train_processed[target_col]
        y_test = test_processed[target_col]
        
        print(f"\nCategorical features ready: {X_train_cat.shape}")
        print(f"Features: {categorical_features}")
        
        return X_train_cat, X_test_cat, y_train, y_test, categorical_features
    else:
        print("No categorical features to process")
        empty_df_train = pd.DataFrame(index=train_processed.index)
        empty_df_test = pd.DataFrame(index=test_processed.index)
        return empty_df_train, empty_df_test, train_processed[target_col], test_processed[target_col], []

def preprocess_continuous_features(train_df, test_df, continuous_cols, target_col):
    """
    Preprocess continuous features for modeling
    """
    print("\n" + "="*60)
    print("3. PREPROCESSING CONTINUOUS FEATURES")
    print("="*60)
    
    train_processed = train_df.copy()
    test_processed = test_df.copy()
    
    # Drop leaky continuous features
    leaky_patterns = [
        'view', 'opened', 'clicked', 'responded', 'post', 'status', 'error',
        'api_calls_made', 'distribution_id', 'enrichment_status', 'enrichment_error'
    ]
    leaky_continuous = [c for c in continuous_cols if any(p in c.lower() for p in leaky_patterns) and c != target_col]
    if leaky_continuous:
        print(f"Dropping leaky continuous features: {leaky_continuous}")
        train_processed = train_processed.drop(columns=leaky_continuous, errors='ignore')
        test_processed = test_processed.drop(columns=leaky_continuous, errors='ignore')
        continuous_cols = [c for c in continuous_cols if c not in leaky_continuous]
    
    continuous_features = []
    
    # Only allow derived features for a small set of domain-relevant columns
    allowed_derived_patterns = ['revenue', 'headcount', 'growth', 'employees', 'size']
    
    for col in continuous_cols:
        if col in train_processed.columns and col != target_col:
            # Handle missing values with median
            median_val = train_processed[col].median()
            train_processed[col] = train_processed[col].fillna(median_val)
            test_processed[col] = test_processed[col].fillna(median_val)
            
            # Add missing indicator if significant missing values
            missing_pct = train_df[col].isnull().sum() / len(train_df) * 100
            if missing_pct > 10:
                train_processed[f'{col}_missing'] = train_df[col].isnull().astype(int)
                test_processed[f'{col}_missing'] = test_df[col].isnull().astype(int)
                continuous_features.append(f'{col}_missing')
                print(f"Added missing indicator for {col} ({missing_pct:.1f}% missing)")
            
            continuous_features.append(col)
            
            # Create derived features only for allowed columns
            if any(p in col.lower() for p in allowed_derived_patterns) and train_processed[col].var() > 0:
                # Log transform for positive values
                if train_processed[col].min() > 0:
                    train_processed[f'{col}_log'] = np.log1p(train_processed[col])
                    test_processed[f'{col}_log'] = np.log1p(test_processed[col])
                    continuous_features.append(f'{col}_log')
                    print(f"Created log transform for {col}")
    
    if continuous_features:
        # Select only continuous features and target
        X_train_cont = train_processed[continuous_features]
        X_test_cont = test_processed[continuous_features]
        y_train = train_processed[target_col]
        y_test = test_processed[target_col]
        
        # Scale continuous features
        scaler = StandardScaler()
        X_train_cont_scaled = pd.DataFrame(
            scaler.fit_transform(X_train_cont),
            columns=X_train_cont.columns,
            index=X_train_cont.index
        )
        X_test_cont_scaled = pd.DataFrame(
            scaler.transform(X_test_cont),
            columns=X_test_cont.columns,
            index=X_test_cont.index
        )
        
        print(f"\nContinuous features ready: {X_train_cont_scaled.shape}")
        print(f"Features: {continuous_features}")
        
        return X_train_cont_scaled, X_test_cont_scaled, y_train, y_test, continuous_features
    else:
        print("No continuous features to process")
        empty_df_train = pd.DataFrame(index=train_processed.index)
        empty_df_test = pd.DataFrame(index=test_processed.index)
        return empty_df_train, empty_df_test, train_processed[target_col], test_processed[target_col], []

def train_and_evaluate_models(X_train_cat, X_test_cat, X_train_cont, X_test_cont, y_train, y_test, categorical_features, continuous_features):
    """
    Train separate models for categorical and continuous features
    """
    print("\n" + "="*60)
    print("4. TRAINING AND EVALUATING MODELS")
    print("="*60)

    results = {}
    
    # Model 1: Categorical Features Only
    if len(categorical_features) > 0 and X_train_cat.shape[1] > 0:
        print("\n--- CATEGORICAL MODEL ---")
        
        # Feature selection for categorical (handle case where chi2 might fail)
        try:
            selector_cat = SelectKBest(score_func=chi2, k=min(10, len(categorical_features)))
            X_train_cat_selected = selector_cat.fit_transform(X_train_cat, y_train)
            X_test_cat_selected = selector_cat.transform(X_test_cat)
            selected_cat_features = np.array(categorical_features)[selector_cat.get_support()]
        except:
            # Fallback to mutual info if chi2 fails
            selector_cat = SelectKBest(score_func=mutual_info_classif, k=min(10, len(categorical_features)))
            X_train_cat_selected = selector_cat.fit_transform(X_train_cat, y_train)
            X_test_cat_selected = selector_cat.transform(X_test_cat)
            selected_cat_features = np.array(categorical_features)[selector_cat.get_support()]
        
        print(f"Selected categorical features: {list(selected_cat_features)}")
        
        # Train categorical model
        cat_model = RandomForestClassifier(
            n_estimators=100,
            max_depth=8,
            min_samples_split=5,
            random_state=42,
            class_weight='balanced'
        )
        cat_model.fit(X_train_cat_selected, y_train)
        
        # Evaluate categorical model
        cat_train_pred = cat_model.predict(X_train_cat_selected)
        cat_test_pred = cat_model.predict(X_test_cat_selected)
        
        cat_train_accuracy = accuracy_score(y_train, cat_train_pred)
        cat_test_accuracy = accuracy_score(y_test, cat_test_pred)
        
        # Feature importance for categorical
        cat_importance = pd.DataFrame({
            'feature': selected_cat_features,
            'importance': cat_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        results['categorical'] = {
            'model': cat_model,
            'train_accuracy': cat_train_accuracy,
            'test_accuracy': cat_test_accuracy,
            'feature_importance': cat_importance,
            'selected_features': list(selected_cat_features)
        }
        
        print(f"Categorical Model - Train Accuracy: {cat_train_accuracy:.3f}")
        print(f"Categorical Model - Test Accuracy: {cat_test_accuracy:.3f}")
        print("Top 10 categorical features by importance:")
        for i, (_, row) in enumerate(cat_importance.head(10).iterrows()):
            print(f"  {i+1}. {row['feature']}: {row['importance']:.4f}")
        print("\nTop 3 categorical drivers:")
        for i, (_, row) in enumerate(cat_importance.head(3).iterrows()):
            print(f"  {i+1}. {row['feature']} (importance: {row['importance']:.4f}) is a key driver of predictions.")
    
    # Model 2: Continuous Features Only
    if len(continuous_features) > 0 and X_train_cont.shape[1] > 0:
        print("\n--- CONTINUOUS MODEL ---")
        
        # Feature selection for continuous
        selector_cont = SelectKBest(score_func=f_classif, k=min(10, len(continuous_features)))
        X_train_cont_selected = selector_cont.fit_transform(X_train_cont, y_train)
        X_test_cont_selected = selector_cont.transform(X_test_cont)
        
        selected_cont_features = np.array(continuous_features)[selector_cont.get_support()]
        print(f"Selected continuous features: {list(selected_cont_features)}")
        
        # Train continuous model
        cont_model = RandomForestClassifier(
            n_estimators=100,
            max_depth=8,
            min_samples_split=5,
            random_state=42,
            class_weight='balanced'
        )
        cont_model.fit(X_train_cont_selected, y_train)
        
        # Evaluate continuous model
        cont_train_pred = cont_model.predict(X_train_cont_selected)
        cont_test_pred = cont_model.predict(X_test_cont_selected)
        
        cont_train_accuracy = accuracy_score(y_train, cont_train_pred)
        cont_test_accuracy = accuracy_score(y_test, cont_test_pred)
        
        # Feature importance for continuous
        cont_importance = pd.DataFrame({
            'feature': selected_cont_features,
            'importance': cont_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        results['continuous'] = {
            'model': cont_model,
            'train_accuracy': cont_train_accuracy,
            'test_accuracy': cont_test_accuracy,
            'feature_importance': cont_importance,
            'selected_features': list(selected_cont_features)
        }
        
        print(f"Continuous Model - Train Accuracy: {cont_train_accuracy:.3f}")
        print(f"Continuous Model - Test Accuracy: {cont_test_accuracy:.3f}")
        print("Top 10 continuous features by importance:")
        for i, (_, row) in enumerate(cont_importance.head(10).iterrows()):
            print(f"  {i+1}. {row['feature']}: {row['importance']:.4f}")
        print("\nTop 3 continuous drivers:")
        for i, (_, row) in enumerate(cont_importance.head(3).iterrows()):
            print(f"  {i+1}. {row['feature']} (importance: {row['importance']:.4f}) is a key driver of predictions.")
    
    # Model 3: Combined Model
    if (len(categorical_features) > 0 and X_train_cat.shape[1] > 0) or (len(continuous_features) > 0 and X_train_cont.shape[1] > 0):
        print("\n--- COMBINED MODEL ---")
        X_train_combined = pd.concat([X_train_cat, X_train_cont], axis=1)
        X_test_combined = pd.concat([X_test_cat, X_test_cont], axis=1)
        
        if X_train_combined.shape[1] > 0:
            # Feature selection for combined
            selector_combined = SelectKBest(score_func=f_classif, k=min(15, X_train_combined.shape[1]))
            X_train_combined_selected = selector_combined.fit_transform(X_train_combined, y_train)
            X_test_combined_selected = selector_combined.transform(X_test_combined)
            
            selected_combined_features = X_train_combined.columns[selector_combined.get_support()]
            
            # Train combined model
            combined_model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                random_state=42,
                class_weight='balanced'
            )
            combined_model.fit(X_train_combined_selected, y_train)
            
            # Evaluate combined model
            combined_train_pred = combined_model.predict(X_train_combined_selected)
            combined_test_pred = combined_model.predict(X_test_combined_selected)
            
            combined_train_accuracy = accuracy_score(y_train, combined_train_pred)
            combined_test_accuracy = accuracy_score(y_test, combined_test_pred)
            
            # Feature importance for combined
            combined_importance = pd.DataFrame({
                'feature': selected_combined_features,
                'importance': combined_model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            results['combined'] = {
                'model': combined_model,
                'train_accuracy': combined_train_accuracy,
                'test_accuracy': combined_test_accuracy,
                'feature_importance': combined_importance,
                'selected_features': list(selected_combined_features)
            }
            
            print(f"Combined Model - Train Accuracy: {combined_train_accuracy:.3f}")
            print(f"Combined Model - Test Accuracy: {combined_test_accuracy:.3f}")
            print("Top 10 combined features by importance:")
            for i, (_, row) in enumerate(combined_importance.head(10).iterrows()):
                print(f"  {i+1}. {row['feature']}: {row['importance']:.4f}")
            print("\nTop 3 combined drivers:")
            for i, (_, row) in enumerate(combined_importance.head(3).iterrows()):
                print(f"  {i+1}. {row['feature']} (importance: {row['importance']:.4f}) is a key driver of predictions.")
    
    return results

def create_visualizations_and_summary(results, test_df, target_col):
    """
    Create visualizations and comprehensive summary
    """
    print("\n" + "="*60)
    print("5. CREATING VISUALIZATIONS AND SUMMARY")
    print("="*60)
    
    if not results:
        print("No models to visualize")
        return {}
    
    # Model comparison chart
    model_names = []
    train_accuracies = []
    test_accuracies = []
    
    for model_type, result in results.items():
        model_names.append(model_type.capitalize())
        train_accuracies.append(result['train_accuracy'])
        test_accuracies.append(result['test_accuracy'])
    
    # Create comparison chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Accuracy comparison
    x = np.arange(len(model_names))
    width = 0.35
    
    ax1.bar(x - width/2, train_accuracies, width, label='Training Accuracy', alpha=0.8)
    ax1.bar(x + width/2, test_accuracies, width, label='Test Accuracy', alpha=0.8)
    ax1.set_xlabel('Model Type')
    ax1.set_ylabel('Accuracy')
    ax1.set_title('Model Performance Comparison')
    ax1.set_xticks(x)
    ax1.set_xticklabels(model_names)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Feature importance comparison (top 10 from combined model or best available)
    importance_data = None
    if 'combined' in results:
        importance_data = results['combined']['feature_importance']
    elif results:
        # Use the first available model
        importance_data = list(results.values())[0]['feature_importance']
    
    if importance_data is not None:
        top_features = importance_data.head(10)
        ax2.barh(range(len(top_features)), top_features['importance'])
        ax2.set_yticks(range(len(top_features)))
        ax2.set_yticklabels(top_features['feature'])
        ax2.set_xlabel('Feature Importance')
        ax2.set_title('Top 10 Feature Importance')
        ax2.invert_yaxis()
    
    plt.tight_layout()
    plt.savefig('static/charts/model_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Create summary report
    summary = {
        'test_data_analysis': {
            'total_test_records': len(test_df),
            'target_distribution': test_df[target_col].value_counts().to_dict(),
            'target_rate': test_df[target_col].mean()
        },
        'model_performance': {},
        'feature_insights': {},
        'recommendations': []
    }
    
    # Add model performance to summary
    best_model = None
    best_accuracy = 0
    
    for model_type, result in results.items():
        summary['model_performance'][model_type] = {
            'train_accuracy': result['train_accuracy'],
            'test_accuracy': result['test_accuracy'],
            'top_features': result['feature_importance'].head(5).to_dict('records')
        }
        
        if result['test_accuracy'] > best_accuracy:
            best_accuracy = result['test_accuracy']
            best_model = model_type
    
    summary['best_model'] = best_model
    summary['best_accuracy'] = best_accuracy
    
    # Add feature insights
    if importance_data is not None:
        summary['feature_insights'] = {
            'most_important_feature': importance_data.iloc[0]['feature'],
            'most_important_importance': importance_data.iloc[0]['importance'],
            'top_10_features': importance_data.head(10).to_dict('records')
        }
    
    # Add recommendations
    recommendations = []
    if best_model:
        recommendations.append(f"Best performing model: {best_model} (Test Accuracy: {best_accuracy:.1%})")
    
    if 'combined' in results:
        top_feature = results['combined']['feature_importance'].iloc[0]['feature']
        recommendations.append(f"Most important factor: {top_feature}")
    
    if 'categorical' in results and 'continuous' in results:
        cat_acc = results['categorical']['test_accuracy']
        cont_acc = results['continuous']['test_accuracy']
        if cat_acc > cont_acc:
            recommendations.append("Categorical features are more predictive than continuous features")
        else:
            recommendations.append("Continuous features are more predictive than categorical features")
    
    summary['recommendations'] = recommendations
    
    # Save summary
    with open('model_analysis_summary.json', 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    # Save feature importance CSV files
    for model_type, result in results.items():
        result['feature_importance'].to_csv(f'feature_importance_{model_type}.csv', index=False)
    
    print("Summary created successfully!")
    if best_model:
        print(f"Best model: {best_model} with {best_accuracy:.1%} accuracy on test data")
    
    return summary

def main(csv_path):
    """
    Main analysis pipeline for real data with separate model testing
    """
    print("REAL DATA B2B ANALYSIS WITH SEPARATE MODEL TESTING")
    print("="*60)

    # 1. Load and analyze real data
    train_df, test_df, target_col, categorical_cols, continuous_cols, full_df = load_and_analyze_real_data(csv_path)
    if train_df is None or test_df is None or target_col is None:
        print("ERROR: Data loading or target detection failed. Exiting.")
        return

    # 2. Preprocess categorical features
    X_train_cat, X_test_cat, y_train, y_test, categorical_features = preprocess_categorical_features(
        train_df, test_df, categorical_cols, target_col
    )
    print("\nFinal categorical features used for modeling:")
    print(list(X_train_cat.columns))

    # 3. Preprocess continuous features
    X_train_cont, X_test_cont, y_train, y_test, continuous_features = preprocess_continuous_features(
        train_df, test_df, continuous_cols, target_col
    )
    print("\nFinal continuous features used for modeling:")
    print(list(X_train_cont.columns))

    # 4. Train and evaluate models
    results = train_and_evaluate_models(
        X_train_cat, X_test_cat, X_train_cont, X_test_cont,
        y_train, y_test, categorical_features, continuous_features
    )
    print("\nFinal combined features used for modeling:")
    print(list(X_train_cat.columns) + list(X_train_cont.columns))

    # 5. Create visualizations and summary
    summary = create_visualizations_and_summary(results, test_df, target_col)

    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("Generated files:")
    print("  - static/charts/model_comparison.png")
    print("  - model_analysis_summary.json")
    for model_type in results:
        print(f"  - feature_importance_{model_type}.csv")
    if summary and 'best_model' in summary:
        print(f"\nBest model: {summary['best_model']} (Test Accuracy: {summary['best_accuracy']:.1%})")
    print("\nTest Results:")
    for model_type, result in results.items():
        print(f"  {model_type.capitalize()} Model: {result['test_accuracy']:.1%} accuracy")

if __name__ == "__main__":
    # Replace with your actual CSV path if needed
    csv_path = "enriched_leads.csv"
    main(csv_path)