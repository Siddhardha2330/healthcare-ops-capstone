CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE doctors (
    doctor_id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(150) NOT NULL,
    department_id INT NOT NULL,
    specialization VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(150),
    status VARCHAR(30) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_doctors_department
        FOREIGN KEY (department_id)
        REFERENCES departments(department_id)
);

CREATE TABLE patients (
    patient_id SERIAL PRIMARY KEY,
    patient_name VARCHAR(150) NOT NULL,
    gender VARCHAR(20),
    date_of_birth DATE,
    phone VARCHAR(20),
    email VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE appointments (
    appointment_id SERIAL PRIMARY KEY,
    patient_id INT NOT NULL,
    doctor_id INT NOT NULL,
    appointment_datetime TIMESTAMP NOT NULL,
    checkin_datetime TIMESTAMP,
    consultation_start_datetime TIMESTAMP,
    consultation_end_datetime TIMESTAMP,
    appointment_status VARCHAR(40) NOT NULL,
    no_show_flag BOOLEAN DEFAULT FALSE,
    reason_for_visit VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_appointments_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),
    CONSTRAINT fk_appointments_doctor
        FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id)
);

CREATE TABLE billing (
    bill_id SERIAL PRIMARY KEY,
    appointment_id INT NOT NULL,
    patient_id INT NOT NULL,
    bill_amount NUMERIC(10,2) NOT NULL,
    payment_status VARCHAR(40) NOT NULL,
    payment_method VARCHAR(50),
    billing_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_billing_appointment
        FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id),
    CONSTRAINT fk_billing_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),
    CONSTRAINT chk_bill_amount_positive
        CHECK (bill_amount >= 0)
);

CREATE TABLE diagnostics (
    diagnostic_id SERIAL PRIMARY KEY,
    appointment_id INT NOT NULL,
    patient_id INT NOT NULL,
    test_name VARCHAR(150) NOT NULL,
    test_category VARCHAR(100),
    result_status VARCHAR(50),
    test_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_diagnostics_appointment
        FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id),
    CONSTRAINT fk_diagnostics_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id)
);

CREATE TABLE prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    appointment_id INT NOT NULL,
    patient_id INT NOT NULL,
    doctor_id INT NOT NULL,
    medicine_name VARCHAR(150) NOT NULL,
    dosage VARCHAR(100),
    duration_days INT,
    prescribed_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_prescriptions_appointment
        FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id),
    CONSTRAINT fk_prescriptions_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),
    CONSTRAINT fk_prescriptions_doctor
        FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id)
);

CREATE TABLE feedback (
    feedback_id SERIAL PRIMARY KEY,
    appointment_id INT NOT NULL,
    patient_id INT NOT NULL,
    rating INT,
    comments TEXT,
    feedback_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_feedback_appointment
        FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id),
    CONSTRAINT fk_feedback_patient
        FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id),
    CONSTRAINT chk_rating_range
        CHECK (rating BETWEEN 1 AND 5)
);

